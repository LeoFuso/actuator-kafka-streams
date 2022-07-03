package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Health indicator for Kafka Streams.
 */
public class KafkaStreamsHealthIndicator extends AbstractHealthIndicator {

    private static final String KEY = "KafkaStream";

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    /**
     * Create a new {@link KafkaStreamsHealthIndicator} instance.
     * @param streamsBuilderFactoryBean used to access the underlying {@link KafkaStreams} instance.
     */
    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
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
                   .unknown();
            return;
        }

        final Properties configurationProperties = streamsBuilderFactoryBean.getStreamsConfiguration();
        final Optional<String> applicationId =
                Optional.ofNullable(configurationProperties)
                        .map(config -> (String) config.get(StreamsConfig.APPLICATION_ID_CONFIG));

        if (applicationId.isEmpty()) {
            builder.withDetail(KEY, "Application.id wasn't supplied.")
                   .unknown();
            return;
        }

        final Map<String, Object> details = new HashMap<>();
        final Map<String, Object> perThreadDetails = new HashMap<>();

        final Set<ThreadMetadata> threadMetadata = kafkaStreams.metadataForLocalThreads();
        for (ThreadMetadata metadata : threadMetadata) {
            perThreadDetails.put("threadName", metadata.threadName());
            perThreadDetails.put("threadState", metadata.threadState());
            perThreadDetails.put("adminClientId", metadata.adminClientId());
            perThreadDetails.put("consumerClientId", metadata.consumerClientId());
            perThreadDetails.put("restoreConsumerClientId", metadata.restoreConsumerClientId());
            perThreadDetails.put("producerClientIds", metadata.producerClientIds());
            perThreadDetails.put("activeTasks", taskDetails(metadata.activeTasks()));
            perThreadDetails.put("standbyTasks", taskDetails(metadata.standbyTasks()));
        }

        details.put(applicationId.get(), perThreadDetails);

        final KafkaStreams.State streamState = kafkaStreams.state();
        boolean isRunning = streamState.isRunningOrRebalancing();

        if (!isRunning) {
            final String diagnosticMessage = String.format(
                    "[ %s ] is down: Stream state [ %s ]",
                    applicationId,
                    streamState
            );
            details.put(KEY, diagnosticMessage);
        }

        builder.withDetails(details);
        builder.status(isRunning ? Status.UP : Status.DOWN);
    }

    private static Map<String, Object> taskDetails(Set<TaskMetadata> taskMetadata) {
        final Map<String, Object> details = new HashMap<>();
        for (TaskMetadata metadata : taskMetadata) {
            details.put("taskId", metadata.taskId());
            if (details.containsKey("partitions")) {
                @SuppressWarnings("unchecked")
                List<String> partitionsInfo = (List<String>) details.get("partitions");
                partitionsInfo.addAll(addPartitionsInfo(metadata));
            } else {
                details.put("partitions", addPartitionsInfo(metadata));
            }
        }
        return details;
    }

    private static List<String> addPartitionsInfo(TaskMetadata metadata) {
        return metadata.topicPartitions()
                       .stream()
                       .map(p -> "partition=" + p.partition() + ", topic=" + p.topic())
                       .collect(Collectors.toList());
    }
}
