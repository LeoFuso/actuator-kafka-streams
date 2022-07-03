package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

public class KafkaStreamsHealthIndicator extends AbstractHealthIndicator implements DisposableBean {

    public static final String REPLICATION_PROPERTY = "transaction.state.log.replication.factor";

    private final AdminClient adminClient;
    private final DescribeClusterOptions describeOptions;
    private final boolean considerReplicationFactor;


    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public KafkaStreamsHealthIndicator(
            StreamsBuilderFactoryBean streamsBuilderFactoryBean,
            KafkaProperties properties,
            boolean considerReplicationFactor
    ) {
        final Map<String, Object> adminProperties = properties.buildAdminProperties();
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.adminClient = AdminClient.create(adminProperties);
        this.describeOptions = new DescribeClusterOptions()
                .timeoutMs(10000);
        this.considerReplicationFactor = considerReplicationFactor;
    }


    @Override
    public void destroy() {
        final Duration timeout = Duration.ofSeconds(30);
        adminClient.close(timeout);
    }

    @Override
    protected void doHealthCheck(final Health.Builder builder) throws Exception {

        final DescribeClusterResult result = adminClient.describeCluster(describeOptions);
        final String brokerId = result.controller().get().idString();
        final int nodesNumber = result.nodes().get().size();
        if (considerReplicationFactor) {
            int replicationFactor = getReplicationFactor(brokerId);
            builder.withDetail("requiredNodes", replicationFactor);

            final boolean notEnoughNodes = nodesNumber >= replicationFactor;
            if (notEnoughNodes) {
                builder.status(Status.DOWN);
                return;
            }
        }

        builder.withDetail("clusterId", result.clusterId().get())
               .withDetail("brokerId", brokerId)
               .withDetail("nodes", nodesNumber);

        try {
            buildStreamDetails(builder);
        } catch (Exception e) {
            builder.withDetail("No stream information available", "Kafka broker is not reachable");
            builder.status(Status.DOWN);
            builder.withException(e);
        }
    }

    private int getReplicationFactor(String brokerId) throws Exception {
        final ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        final Map<ConfigResource, Config> kafkaConfig = adminClient
                .describeConfigs(Collections.singletonList(configResource)).all().get();
        final Config brokerConfig = kafkaConfig.get(configResource);
        return Integer.parseInt(brokerConfig.get(REPLICATION_PROPERTY).value());
    }

    private void buildStreamDetails(final Health.Builder builder) {

        final Map<String, Object> details = new HashMap<>();
        final Map<String, Object> perAppIdDetails = new HashMap<>();

        final KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        Objects.requireNonNull(kafkaStreams);

        final Properties configurationProperties = streamsBuilderFactoryBean.getStreamsConfiguration();
        final String applicationId =
                Optional.ofNullable(configurationProperties)
                        .map(config -> (String) config.get(StreamsConfig.APPLICATION_ID_CONFIG))
                        .orElseThrow();

        final Set<ThreadMetadata> threadMetadata = kafkaStreams.metadataForLocalThreads();
        for (ThreadMetadata metadata : threadMetadata) {
            perAppIdDetails.put("threadName", metadata.threadName());
            perAppIdDetails.put("threadState", metadata.threadState());
            perAppIdDetails.put("adminClientId", metadata.adminClientId());
            perAppIdDetails.put("consumerClientId", metadata.consumerClientId());
            perAppIdDetails.put("restoreConsumerClientId", metadata.restoreConsumerClientId());
            perAppIdDetails.put("producerClientIds", metadata.producerClientIds());
            perAppIdDetails.put("activeTasks", taskDetails(metadata.activeTasks()));
            perAppIdDetails.put("standbyTasks", taskDetails(metadata.standbyTasks()));
        }

        details.put(applicationId, perAppIdDetails);

        final KafkaStreams.State overallState = kafkaStreams.state();
        boolean isRunning = overallState.isRunningOrRebalancing();

        final Boolean streamState =
                kafkaStreams.metadataForLocalThreads()
                            .stream()
                            .map(ThreadMetadata::threadState)
                            .map(state -> state.equalsIgnoreCase("RUNNING"))
                            .reduce(Boolean::logicalAnd)
                            .orElse(true);

        if (!isRunning || !streamState) {
            final String diagnosticMessage = String.format(
                    "Application.id  [ %s ] is down. Overall state [ %s ], Stream state [ %s ]",
                    applicationId,
                    overallState,
                    streamState
            );
            details.put("state", diagnosticMessage);
        }

        builder.withDetails(details);
        builder.status(isRunning && streamState ? Status.UP : Status.DOWN);
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
