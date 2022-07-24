package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import javax.annotation.Nullable;

import java.time.Duration;
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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

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
                                          .orElse(1);
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
            logger.warn("Could not access the current lag: KafkaStreams not ready.");
            return record;
        }

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
            logger.error("Upscale failure. StreamsBuilderFactoryBean not started yet.");
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
            logger.error("Upscale failure. StreamsBuilderFactoryBean not started yet.");
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
        final long periodInMillis = period.toMillis();
        final long initialDelay = periodInMillis / 2;
        executor.scheduleAtFixedRate(this, initialDelay, periodInMillis, TimeUnit.MILLISECONDS);
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
        final Map<String, Map<TopicPartition, Long>> recordedLag = lag();
        final int threadCount = recordedLag.size();
        if (threadCount == 0) {
            logger.warn("Skipping loop. Could not access the recorded lag.");
            return;
        }

        final Float lag = recordedLag
                .values()
                .stream()
                .flatMap(thread -> {
                    final Collection<Long> threadLag = thread.values();
                    return threadLag.stream();
                })
                .reduce(Long::sum)
                .map(accumulatedLag -> accumulatedLag / (float) threadCount)
                .orElse(0.0f);

        final long threshold = properties.getLagThreshold();
        if (lag > threshold) {
            final Integer threadLimit = properties.getStreamThreadLimit();
            if (threadCount >= threadLimit) {
                logger.warn(
                        "StreamThread count [{}] is above limit [{}]. Autopilot will not create new StreamThreads.",
                        threadCount,
                        threadLimit
                );
            } else {
                addStreamThread();
            }
            return;
        }

        if (threadCount > desiredThreadCount) {
            removeStreamThread();
        }
    }
}
