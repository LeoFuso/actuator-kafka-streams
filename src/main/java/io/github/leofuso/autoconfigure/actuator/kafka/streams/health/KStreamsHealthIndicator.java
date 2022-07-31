package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.ConfigUtils;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.configDef;

/**
 * Health indicator for Kafka Streams.
 */
public class KStreamsHealthIndicator extends AbstractHealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(KStreamsHealthIndicator.class);

    /**
     * Used to extract info from {@link KafkaStreams} instance.
     */
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    /**
     * Either or not to use the {@code num.stream.threads} config as a threshold as the required minimum number of
     * live {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads}.
     */
    private final boolean allowThreadLoss;

    /**
     * A minimum number of live {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads}
     * required to indicate a {@link Status#UP healthy} status.
     */
    private final int minNumOfLiveStreamThreads;

    /**
     * Helper to format huge numbers, commonly placed in situations of high partition-lag.
     */
    private static final NumberFormat numberFormat = NumberFormat.getCompactNumberInstance(
            Locale.US,
            NumberFormat.Style.SHORT
    );

    /**
     * Create a new KStreamsHealthIndicator instance.
     *
     * @param factory    used to access the underlying {@link KafkaStreams} instance.
     * @param properties used to configure the health-check.
     */
    public KStreamsHealthIndicator(StreamsBuilderFactoryBean factory, KStreamsIndicatorProperties properties) {
        this(factory, properties.isAllowThreadLoss(), properties.getMinimumNumberOfLiveStreamThreads());
    }

    /**
     * Create a new instance.
     *
     * @param factory         used to access the underlying {@link KafkaStreams} instance.
     * @param allowThreadLoss either or not to use the {@code num.stream.threads} as a threshold.
     * @param minNumOfThreads a minimum number of live
     *                        {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads}.
     */
    public KStreamsHealthIndicator(StreamsBuilderFactoryBean factory, boolean allowThreadLoss, int minNumOfThreads) {
        this.streamsBuilderFactoryBean = factory;
        this.allowThreadLoss = allowThreadLoss;
        this.minNumOfLiveStreamThreads = minNumOfThreads;
    }

    @Override
    protected void doHealthCheck(final Health.Builder builder) {
        try {
            buildStreamDetails(builder);
        } catch (Exception e) {
            logger.error("Health-check failure:", e);
            builder.unknown();
        }
    }

    private void buildStreamDetails(final Health.Builder builder) {

        final KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            builder.withDetail("diagnostic", "StreamBuilderFactoryBean probably hasn't fully started yet.")
                   .down();
            return;
        }

        final Properties configurationProperties = streamsBuilderFactoryBean.getStreamsConfiguration();
        final Optional<String> applicationId =
                Optional.ofNullable(configurationProperties)
                        .map(config -> (String) config.get(APPLICATION_ID_CONFIG));

        if (applicationId.isEmpty()) {
            builder.withDetail("diagnostic", "[application.id] property wasn't supplied.")
                   .down();
            return;
        }

        final Map<String, Object> details = new HashMap<>();

        details.put("application.id", applicationId.get());
        details.put("threads", threadDetails(kafkaStreams));

        final KafkaStreams.State state = kafkaStreams.state();
        details.put("state", state);

        boolean isRunning = state.isRunningOrRebalancing();
        final boolean hasMinimumThreadCount = hasMinimumThreadCount(kafkaStreams);
        if (!hasMinimumThreadCount) {
            final String diagnosticMessage = String.format(
                    "App did not reach the required minimum number of live Stream Thread: %s.",
                    allowThreadLoss ? minNumOfLiveStreamThreads: "[num.stream.threads] property"
            );
            details.put("minimum-number-of-live-stream-threads", diagnosticMessage);
        }

        builder.withDetails(details);
        builder.status(isRunning && hasMinimumThreadCount ? Status.UP : Status.DOWN);
    }


    private List<Map<String, Object>> threadDetails(KafkaStreams kafkaStreams) {
        return kafkaStreams
                .metadataForLocalThreads()
                .stream()
                .sorted(Comparator.comparing(ThreadMetadata::threadName))
                .map(metadata -> Map.of(
                        "thread.name", metadata.threadName(),
                        "thread.state", metadata.threadState(),
                        "admin.client.id", metadata.adminClientId(),
                        "consumer.client.id", metadata.consumerClientId(),
                        "restore.consumer.client.id", metadata.restoreConsumerClientId(),
                        "producer.client.ids", metadata.producerClientIds(),
                        "active.tasks", taskDetails(metadata.activeTasks()),
                        "standby.tasks", taskDetails(metadata.standbyTasks())
                ))
                .toList();
    }

    private List<Map<String, Object>> taskDetails(Set<TaskMetadata> taskMetadata) {
        final String taskIdKey = "task.id";
        return taskMetadata.stream()
                           .map(metadata -> Map.of(
                                   taskIdKey, "%s".formatted(metadata.taskId()),
                                   "partitions", addPartitionsInfo(metadata)
                           ))
                           .sorted(Comparator.comparing(map -> (String) map.get(taskIdKey)))
                           .toList();
    }

    private List<Map<String, Object>> addPartitionsInfo(TaskMetadata metadata) {
        return metadata
                .topicPartitions()
                .stream()
                .map(tp -> {

                    final Map<TopicPartition, Long> committedMap = metadata.committedOffsets();
                    final Map<TopicPartition, Long> offsetMap = metadata.endOffsets();

                    final long committedOffset = committedMap.get(tp);
                    final long endOffset = offsetMap.get(tp);
                    final long lag = Math.max(0, endOffset - committedOffset);

                    @SuppressWarnings("UnnecessaryLocalVariable")
                    final Map<String, Object> entries = Map.ofEntries(
                            Map.entry("partition", "%s".formatted(tp)),
                            Map.entry("committed.offset", committedOffset),
                            Map.entry("end.offset", endOffset),
                            Map.entry("lag", numberFormat.format(lag))
                    );

                    return entries;
                })
                .sorted(Comparator.comparing(map -> (String) map.get("partition")))
                .toList();
    }

    private boolean hasMinimumThreadCount(KafkaStreams kafkaStreams) {

        final long liveThreads = kafkaStreams
                .metadataForLocalThreads()
                .stream()
                .map(ThreadMetadata::threadState)
                .map(StreamThread.State::valueOf)
                .map(StreamThread.State::isAlive)
                .count();

        if (!allowThreadLoss) {
            final Properties properties = streamsBuilderFactoryBean.getStreamsConfiguration();
            return ConfigUtils.
                    <Integer>access(properties, NUM_STREAM_THREADS_CONFIG, configDef())
                    .map(numThreads -> liveThreads >= numThreads)
                    .orElse(liveThreads != 0);
        }

        return liveThreads >= minNumOfLiveStreamThreads;
    }
}
