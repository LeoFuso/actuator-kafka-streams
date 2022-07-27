package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.text.NumberFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import com.google.common.collect.Sets;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.CompactNumberFormatUtils;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotConfigurationProperties.Period;
import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * A default implementation of the {@link Autopilot autopilot} API.
 */
public class DefaultAutopilot implements Autopilot {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAutopilot.class);

    /**
     * The current {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.Autopilot.State}.
     */
    private State state = State.STAND_BY;

    /**
     * A lock for the {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.Autopilot.State}.
     */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock(true);

    /**
     * Stores the most recent lag info.
     */
    private final Map<String, Map<TopicPartition, Long>> threadInfo = new HashMap<>();


    /**
     * As long as the target is above or bellow the {@link DefaultAutopilot#desiredThreadCount}, an action must be
     * taken.
     */
    private Integer targetThreadCount;

    /**
     * The initial thread count defined by the user.
     */
    private final Integer desiredThreadCount;

    private final RecoveryWindow recoveryWindow;

    /**
     * Used to access the {@link KafkaStreams}.
     */
    private final StreamsBuilderFactoryBean factory;
    /**
     * To coordinate the {@link Autopilot autopilot}.
     */
    private final AutopilotConfigurationProperties properties;

    /**
     * {@link java.util.concurrent.Executor Executor} responsible for running the {@link Autopilot autopilot}.
     */
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final NumberFormat numberFormat = NumberFormat.getCompactNumberInstance(
            Locale.US,
            NumberFormat.Style.SHORT
    );

    /**
     * Creates a new {@link Autopilot} instance. Automation can be activated and deactivated by invoking the
     * {@link Autopilot#initialize() initialize} and {@link Autopilot#shutdown()  shutdown} methods, respectively.
     *
     * @param factory used to access the {@link KafkaStreams} instance.
     * @param prop    that coordinates the {@link Autopilot} operation.
     */
    public DefaultAutopilot(StreamsBuilderFactoryBean factory, AutopilotConfigurationProperties prop) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        this.properties = Objects.requireNonNull(prop, "AutopilotConfigurationProperties [prop] is required.");
        final Properties streamProperties = factory.getStreamsConfiguration();
        this.desiredThreadCount = Optional.ofNullable(streamProperties)
                                          .map(property -> (String) property.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG))
                                          .map(Integer::parseInt)
                                          .orElseGet(() -> {
                                              final ConfigDef definition = StreamsConfig.configDef();
                                              final Map<String, Object> defaultValues = definition.defaultValues();
                                              return (Integer) defaultValues.getOrDefault(
                                                      StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                                                      1
                                              );
                                          });
        this.targetThreadCount = this.desiredThreadCount;
        this.recoveryWindow = Autopilot.recoveryWindow(properties);
    }

    @Override
    public void run() {

        final Long threshold = properties.getLagThreshold();
        logger.info(
                "Autopilot is gathering lag info from all StreamThreads. Looking for partition-lag above [{}].",
                numberFormat.format(threshold)
        );

        final WriteLock lock = stateLock.writeLock();
        final boolean unlocked = lock.tryLock();
        if (!unlocked) {
            logger.info("Autopilot [NOOP]. Could not get lock, is someone else holding it?");
            return;
        }

        try {
            doRun();
        } catch (Exception ex) {
            logger.error("Autopilot [NOOP]. Something went wrong.", ex);
        } finally {
            lock.unlock();
        }
    }

    private void doRun() {

        final Map<String, Map<TopicPartition, Long>> threads = threads();
        if (threads.isEmpty()) {
            return;
        }

        final boolean canActUpon = state.isValidTransition(
                State.BOOSTING,
                State.DECREASING,
                State.STAND_BY
        );

        if (!canActUpon) {
            logger.info("Autopilot [NOOP]. Nothing to be done. State [{}]", state);
            return;
        }

        final boolean isOpen = !recoveryWindow.hasClosed();
        if (isOpen) {
            return;
        }

        final State nextState = decideNextState();
        switch (nextState) {
            case STAND_BY, BOOSTED -> state = nextState;
            case BOOSTING -> {
                state = State.BOOSTING;
                logger.info("Autopilot is [{}] the StreamThread count.", state);
                doAddStreamThread();
            }
            case DECREASING -> {
                state = State.DECREASING;
                logger.info("Autopilot is [{}] the StreamThread count.", state);
                doRemoveStreamThread();
            }
        }
    }

    private State decideNextState() {

        final int threadCount = threadInfo.size();
        final long accumulatedLag = threadInfo
                .values()
                .stream()
                .flatMap(thread -> {
                    final Collection<Long> threadLag = thread.values();
                    return threadLag.stream();
                })
                .reduce(Long::sum)
                .orElse(0L);

        final long average = accumulatedLag / threadCount;
        logger.info("Autopilot found an average partition-lag of {}", numberFormat.format(average));

        final int threadLimit = desiredThreadCount + properties.getStreamThreadLimit();
        if (threadLimit == threadCount) {
            logger.warn(
                    "Autopilot [NOOP]. StreamThread count [{}] has reached the limit [{}].",
                    threadCount,
                    threadLimit
            );
        }

        final Long lagThreshold = properties.getLagThreshold();
        final int upperLimit = desiredThreadCount + threadLimit;
        for (targetThreadCount = desiredThreadCount; targetThreadCount < upperLimit; targetThreadCount++) {
            final long averageLag = accumulatedLag / targetThreadCount;
            if (averageLag <= lagThreshold) {
                break;
            }
        }

        logger.info(
                "Autopilot found StreamThread target count to be {}, current is {}",
                targetThreadCount,
                threadCount);
        return targetThreadCount > threadCount ? State.BOOSTING
                : targetThreadCount < threadCount ? State.DECREASING
                : targetThreadCount.equals(desiredThreadCount) ? State.STAND_BY
                : State.BOOSTED;
    }

    @Override
    public void addStreamThread() {
        if (state != State.BOOSTED && state != State.STAND_BY) {
            final String message = "Autopilot [NOOP]. Cannot manually transition from [%s] to [%s].".formatted(
                    state,
                    State.BOOSTING
            );
            throw new IllegalStateException(message);
        }

        final WriteLock lock = stateLock.writeLock();
        final boolean unlocked = lock.tryLock();
        if (!unlocked) {
            throw new IllegalStateException("Autopilot [NOOP]. Could not get lock, is someone else holding it?");
        }

        try {
            doAddStreamThread();
        } finally {
            lock.unlock();
        }

    }

    private void doAddStreamThread() {

        final KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            logger.error("Autopilot [NOOP]. StreamsBuilderFactoryBean not started yet.");
            return;
        }

        CompletableFuture
                .supplyAsync(streams::addStreamThread)
                .whenCompleteAsync((optional, throwable) -> {

                    threads();
                    if (throwable != null) {
                        logger.error(
                                "Oops, something went wrong. Autopilot couldn't add a new StreamThread.",
                                throwable
                        );
                        return;
                    }

                    final boolean empty = optional.isEmpty();
                    if (empty) {
                        logger.error("Oops, something went wrong. Autopilot couldn't add a new StreamThread.");
                        return;
                    }

                    logger.info("A StreamThread [{}] was successfully added by Autopilot.", optional.get());
                    state = State.BOOSTED;

                }, executor);
    }

    @Override
    public void removeStreamThread() {
        if (state != State.BOOSTED) {
            final String message = "Autopilot [NOOP]. Cannot manually transition from [%s] to [%s].".formatted(
                    state,
                    State.BOOSTING
            );
            throw new IllegalStateException(message);
        }

        final WriteLock lock = stateLock.writeLock();
        final boolean unlocked = lock.tryLock();
        if (!unlocked) {
            throw new IllegalStateException("Autopilot [NOOP]. Could not get lock, is someone else holding it?");
        }

        try {
            doRemoveStreamThread();
        } finally {
            lock.unlock();
        }
    }

    private void doRemoveStreamThread() {
        final KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            logger.error("Autopilot [NOOP]. StreamsBuilderFactoryBean not started yet.");
            return;
        }

        CompletableFuture
                .supplyAsync(() -> {
                    final Period period = properties.getPeriod();
                    final Duration recoveryWindow = period.getRecoveryWindow();
                    return streams.removeStreamThread(recoveryWindow);
                })
                .whenCompleteAsync((optional, throwable) -> {

                    threads();
                    if (throwable != null) {
                        logger.error(
                                "Oops, something went wrong. Autopilot couldn't remove any StreamThread.",
                                throwable);
                        return;
                    }

                    final boolean empty = optional.isEmpty();
                    if (empty) {
                        logger.error("Oops, something went wrong. Autopilot couldn't remove any StreamThread.");
                        return;
                    }

                    logger.info("StreamThread [{}] successfully removed by Autopilot.", optional.get());
                    state = decideNextState();

                }, executor);
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public RecoveryWindow recoveryWindow() {
        return recoveryWindow;
    }

    @Override
    public HashMap<String, Map<TopicPartition, Long>> threads() {
        final HashMap<String, Map<TopicPartition, Long>> threads = new HashMap<>();
        final KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            logger.error("Autopilot [NOOP]. Could not gather lag info, is KafkaStreams running?");
            return threads;
        }

        final Pattern exclusionPattern = properties.getExclusionPattern();
        final Predicate<String> exclusionPredicate = exclusionPattern.asPredicate();

        final Set<ThreadMetadata> localThreads = streams.metadataForLocalThreads();
        for (ThreadMetadata threadMetadata : localThreads) {

            final HashMap<TopicPartition, Long> partitionLag = new HashMap<>();
            final Set<TaskMetadata> activeTasks = threadMetadata.activeTasks();
            final Set<TaskMetadata> standbyTasks = threadMetadata.standbyTasks();

            for (TaskMetadata taskMetadata : Sets.union(activeTasks, standbyTasks)) {

                final Map<TopicPartition, Long> endOffsets = taskMetadata.endOffsets();
                final Map<TopicPartition, Long> committedOffsets = taskMetadata.committedOffsets();

                for (Map.Entry<TopicPartition, Long> endOffsetEntry : endOffsets.entrySet()) {
                    final TopicPartition partition = endOffsetEntry.getKey();

                    final String topic = partition.topic();
                    final boolean isExcluded = exclusionPredicate.test(topic);
                    if (isExcluded) {
                        continue;
                    }

                    final Long endOffset = endOffsetEntry.getValue();
                    final Long committedOffset = committedOffsets.get(partition);

                    if (endOffset <= 0 || committedOffset <= 0) {
                        /* We don't need to store zero-lag or undefined lag (-1) info. */
                        continue;
                    }

                    Long lag = Math.abs(endOffset - committedOffset);
                    partitionLag.put(partition, lag);
                }
            }

            final String name = threadMetadata.threadName();
            threads.put(name, partitionLag);
        }

        final Set<String> managedThreads = threadInfo.keySet();
        final Set<String> updatedThreads = threads.keySet();

        Sets.difference(managedThreads, updatedThreads)
            .forEach(threadInfo::remove);

        threadInfo.putAll(threads);

        if (threadInfo.isEmpty()) {
            logger.warn("Autopilot [NOOP]. Could not gather lag info. No active or inactive Tasks.");
        }

        return threads;
    }

    @Override
    public void initialize() {

        final Period period = properties.getPeriod();
        final Duration initialDelay = period.getInitialDelay();
        final Duration betweenRuns = period.getBetweenRuns();

        final long initialDelayInMillis = initialDelay.toMillis();
        final long betweenRunsInMillis = betweenRuns.toMillis();

        executor.scheduleAtFixedRate(this, initialDelayInMillis, betweenRunsInMillis, TimeUnit.MILLISECONDS);

        final String prettyInitialDelay = CompactNumberFormatUtils.format(initialDelay);
        final String prettyPeriod = CompactNumberFormatUtils.format(betweenRuns);
        logger.info(
                "Autopilot scheduled. Will commence in {} with evaluation periods every {}.",
                prettyInitialDelay,
                prettyPeriod
        );
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
    }

}
