package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import javax.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.CompactNumberFormatUtils;

/**
 * A default implementation of the {@link Autopilot autopilot} API.
 */
public class DefaultAutopilot implements Autopilot {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAutopilot.class);

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
    private final ScheduledExecutorService executor;

    /**
     * The initial thread count defined by the user.
     */
    private final Integer desiredThreadCount;

    /**
     * Used to keep time between actions.
     */
    private Clock clock = Clock.systemUTC();

    /**
     * Timestamp of the last performed action.
     */
    private Instant previousAction = Instant.ofEpochMilli(Long.MIN_VALUE);


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
        this.executor = Executors.newSingleThreadScheduledExecutor();
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
    }

    /**
     * Used only to facilitate unit testing.
     *
     * @param clock a new {@link Clock} to keep track of time between actions.
     */
    public void setClock(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "Clock [clock] is required.");
    }

    @Override
    public boolean shouldBoost(final Map<String, Map<TopicPartition, Long>> lag) {
        final int threadCount = lag.size();
        if (threadCount == 0) {
            return false;
        }

        final double partitionLag = lag
                .values()
                .stream()
                .flatMap(thread -> {
                    final Collection<Long> threadLag = thread.values();
                    return threadLag.stream();
                })
                .mapToLong(v -> v)
                .average()
                .orElse(0.0);

        final long threshold = properties.getLagThreshold();
        if (partitionLag <= threshold) {
            return false;
        }

        final Integer threadLimit = properties.getStreamThreadLimit();
        if (threadCount >= threadLimit) {
            logger.warn(
                    "Autopilot [NOOP]. StreamThread count [{}] has reached the limit [{}]. Will not apply a Boost.",
                    threadCount,
                    threadLimit
            );
            return false;
        }

        final String prettyLag = CompactNumberFormatUtils.format((long) partitionLag);
        logger.info("Autopilot found an average partition-lag of {}", prettyLag);
        return true;
    }

    @Override
    public boolean shouldNerf(final Map<String, Map<TopicPartition, Long>> lag) {
        final int threadCount = lag.size();
        return threadCount > desiredThreadCount;
    }

    @Override
    public void addStreamThread() {
        doAddStreamThread(factory.getKafkaStreams());
    }

    @Override
    public void removeStreamThread() {
        doRemoveStreamThread(factory.getKafkaStreams());
    }

    @Override
    public HashMap<String, Map<TopicPartition, Long>> lag() {
        final HashMap<String, Map<TopicPartition, Long>> record = new HashMap<>();
        final KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            logger.error("Autopilot [NOOP]. Could not access the current lag info, KafkaStreams not ready.");
            return record;
        }

        final Pattern exclusionPattern = properties.getExclusionPattern();
        final Predicate<String> exclusionPredicate = exclusionPattern.asPredicate();

        final Set<ThreadMetadata> localThreads = streams.metadataForLocalThreads();
        for (ThreadMetadata threadMetadata : localThreads) {

            final String rawState = threadMetadata.threadState();
            final StreamThread.State state = StreamThread.State.valueOf(rawState);
            final boolean alive = state.isAlive();
            if (!alive) {
                continue;
            }

            final HashMap<TopicPartition, Long> partitionLag = new HashMap<>();
            final Set<TaskMetadata> activeTasks = threadMetadata.activeTasks();
            for (TaskMetadata taskMetadata : activeTasks) {

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
                    Long committedOffset = committedOffsets.get(partition);

                    Long lag = Math.abs(endOffset - committedOffset);
                    partitionLag.put(partition, lag);
                }
            }

            final String name = threadMetadata.threadName();
            record.put(name, partitionLag);
        }

        return record;
    }

    private void doAddStreamThread(@Nullable KafkaStreams streams) {
        if (streams == null) {
            logger.error("Autopilot [NOOP]. StreamsBuilderFactoryBean not started yet.");
            return;
        }
        streams.addStreamThread()
               .ifPresentOrElse(
                       thread -> logger.info("A StreamThread [{}] was successfully added by Autopilot.", thread),
                       () -> logger.error("Oops, something went wrong. Autopilot couldn't add a new StreamThread.")
               );
    }

    private void doRemoveStreamThread(@Nullable KafkaStreams streams) {
        if (streams == null) {
            logger.error("Autopilot [NOOP]. StreamsBuilderFactoryBean not started yet.");
            return;
        }
        final Duration timeout = properties.getTimeout();
        streams.removeStreamThread(timeout)
               .ifPresentOrElse(
                       thread -> logger.info("StreamThread [{}] successfully removed by Autopilot.", thread),
                       () -> logger.error("Oops, something went wrong. Autopilot couldn't remove any StreamThread.")
               );
    }

    @Override
    public void initialize() {
        final Duration period = properties.getPeriod();
        final Duration initialDelay = period.dividedBy(2);

        final long periodInMillis = period.toMillis();
        final long initialDelayInMillis = initialDelay.toMillis();
        executor.scheduleAtFixedRate(this, initialDelayInMillis, periodInMillis, TimeUnit.MILLISECONDS);

        final String prettyInitialDelay = CompactNumberFormatUtils.format(initialDelay);
        final String prettyPeriod = CompactNumberFormatUtils.format(period);
        logger.info(
                "Autopilot scheduled. Will commence in {} with evaluation periods every {}.",
                prettyInitialDelay,
                prettyPeriod
        );
    }

    @Override
    public void shutdown() throws InterruptedException {
        executor.shutdownNow();
        final Duration timeout = properties.getTimeout();
        @SuppressWarnings("unused")
        final boolean termination = executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {

        logger.info(
                "Autopilot is evaluating all StreamThreads. Looking for partition-lag above [{}].",
                CompactNumberFormatUtils.format(properties.getLagThreshold())
        );

        final Map<String, Map<TopicPartition, Long>> lag = lag();
        final int threadCount = lag.size();
        if (threadCount == 0) {
            logger.error("Autopilot [NOOP]. Could not access any StreamThread lag.");
            return;
        }

        final Instant nextAction = Instant.now(clock);
        final Duration timeout = properties.getTimeout();
        final Duration betweenActions = Duration.between(previousAction, nextAction);
        final boolean canActuate = betweenActions.compareTo(timeout) >= 0;
        if (!canActuate) {

            final Duration period = properties.getPeriod();
            final Duration timeoutRemaining = timeout.minus(betweenActions);
            final Duration remaining = timeoutRemaining.plus(period);

            final String prettyTimeout = CompactNumberFormatUtils.format(timeout);
            final String prettyRemaining = CompactNumberFormatUtils.format(remaining);
            logger.info(
                    "Autopilot [NOOP]. Timeout of {}, will wait for another {}.",
                    prettyTimeout,
                    prettyRemaining
            );
            return;
        }

        previousAction = nextAction;

        if (shouldBoost(lag)) {
            logger.info("Autopilot is applying a [BOOST].");
            addStreamThread();
            return;
        }

        if (shouldNerf(lag)) {
            logger.info("Autopilot is applying a [NERF].");
            removeStreamThread();
        }
    }
}
