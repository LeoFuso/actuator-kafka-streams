package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Health indicator for Kafka Streams.
 */
public class KafkaStreamsHealthIndicator extends AbstractHealthIndicator {

    private static final String KEY = "KafkaStreams";

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    /**
     * Either or not to use the <code>num.stream.threads</code> config as a threshold as the required minimum number of
     * live {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads}.
     */
    private final boolean useNumStreamThreadsAsMinimum;

    /**
     * A minimum number of live {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads}
     * required to indicate a {@link Status#UP healthy} status.
     */
    private final int minNumOfLiveStreamThreads;

    /**
     * Create a new {@link KafkaStreamsHealthIndicator} instance.
     *
     * @param streamsBuilderFactoryBean used to access the underlying {@link KafkaStreams} instance.
     */
    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.useNumStreamThreadsAsMinimum = false;
        this.minNumOfLiveStreamThreads = 1;
    }

    /**
     * Create a new {@link KafkaStreamsHealthIndicator} instance.
     *
     * @param streamsBuilderFactoryBean used to access the underlying {@link KafkaStreams} instance.
     * @param shouldUseNumStreamThreads either or not to use the <code>num.stream.threads</code> as a threshold.
     * @param minNumOfLiveStreamThreads a minimum number of live
     *                                  {@link org.apache.kafka.streams.processor.internals.StreamThread stream
     *                                  threads}.
     */
    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean,
                                       boolean shouldUseNumStreamThreads,
                                       int minNumOfLiveStreamThreads) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.useNumStreamThreadsAsMinimum = shouldUseNumStreamThreads;
        this.minNumOfLiveStreamThreads = minNumOfLiveStreamThreads;
    }

    @Override
    protected void doHealthCheck(final Health.Builder builder) {
        try {
            buildStreamDetails(builder);
        } catch (Exception e) {
            builder.down(e);
        }
    }


    private void buildStreamDetails(final Health.Builder builder) {

        final KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            builder.withDetail(KEY, "StreamBuilderFactoryBean probably hasn't fully started yet.")
                   .down();
            return;
        }

        final Properties configurationProperties = streamsBuilderFactoryBean.getStreamsConfiguration();
        final Optional<String> applicationId =
                Optional.ofNullable(configurationProperties)
                        .map(config -> (String) config.get(StreamsConfig.APPLICATION_ID_CONFIG));

        if (applicationId.isEmpty()) {
            builder.withDetail(KEY, "Application.id wasn't supplied.")
                   .down();
            return;
        }

        final Map<String, Object> details = new HashMap<>();

        details.put("applicationId", applicationId.get());
        threadDetails(kafkaStreams, details);

        final KafkaStreams.State state = kafkaStreams.state();
        boolean isRunning = state.isRunningOrRebalancing();
        if (!isRunning) {
            final String diagnosticMessage = String.format(
                    "[ %s ] is down: KafkaStreams state [ %s ]",
                    applicationId.get(),
                    state
            );
            details.put(KEY, diagnosticMessage);
        }

        final boolean hasMinimumThreadCount = hasMinimumThreadCount(kafkaStreams);
        if (!hasMinimumThreadCount) {
            final String diagnosticMessage = String.format(
                    "[ %s ] did not reach the required minimum number of live threads: [ %s ]",
                    applicationId.get(),
                    useNumStreamThreadsAsMinimum ? "num.stream.threads" : minNumOfLiveStreamThreads
            );
            details.put("minNumberOfLiveThreads", diagnosticMessage);
        }

        builder.withDetails(details);
        builder.status(isRunning && hasMinimumThreadCount ? Status.UP : Status.DOWN);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void threadDetails(KafkaStreams kafkaStreams, Map<String, Object> details) {
        kafkaStreams.metadataForLocalThreads()
                    .stream()
                    .sorted(Comparator.comparing(ThreadMetadata::threadName))
                    .map(metadata -> Map.of(
                            "threadName", metadata.threadName(),
                            "threadState", metadata.threadState(),
                            "adminClientId", metadata.adminClientId(),
                            "consumerClientId", metadata.consumerClientId(),
                            "restoreConsumerClientId", metadata.restoreConsumerClientId(),
                            "producerClientIds", metadata.producerClientIds(),
                            "activeTasks", taskDetails(metadata.activeTasks()),
                            "standbyTasks", taskDetails(metadata.standbyTasks())
                    ))
                    .collect(Collectors.collectingAndThen(
                            Collectors.toUnmodifiableList(),
                            threads -> details.put("threads", threads)
                    ));
    }

    private static Map<String, Object> taskDetails(Set<TaskMetadata> taskMetadata) {
        final String topicPartitionKey = "topicPartitions";
        final Map<String, Object> details = new HashMap<>();
        for (TaskMetadata metadata : taskMetadata) {
            details.put("taskId", metadata.taskId());
            if (details.containsKey(topicPartitionKey)) {
                @SuppressWarnings("unchecked")
                List<String> partitionsInfo = (List<String>) details.get(topicPartitionKey);
                partitionsInfo.addAll(addPartitionsInfo(metadata));
            } else {
                details.put(topicPartitionKey, addPartitionsInfo(metadata));
            }
        }
        return details;
    }

    private static List<String> addPartitionsInfo(TaskMetadata metadata) {
        return metadata.topicPartitions()
                       .stream()
                       .map(TopicPartition::toString)
                       .collect(Collectors.toList());
    }

    private boolean hasMinimumThreadCount(KafkaStreams kafkaStreams) {

        final long liveThreads = kafkaStreams
                .metadataForLocalThreads()
                .stream()
                .map(ThreadMetadata::threadState)
                .map(StreamThread.State::valueOf)
                .map(StreamThread.State::isAlive)
                .count();

        if (useNumStreamThreadsAsMinimum) {
            final Properties properties = streamsBuilderFactoryBean.getStreamsConfiguration();
            return Optional.ofNullable(properties)
                            .map(config -> (String) config.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG))
                            .map(Integer::parseInt)
                            .map(numThreads -> liveThreads >= numThreads)
                            .orElse(liveThreads != 0);
        }

        return liveThreads >= minNumOfLiveStreamThreads;
    }
}
