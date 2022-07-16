package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint.InteractiveQueryEndpointAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint.ReadOnlyStateStoreEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore.StateStoreRestoreEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore.StateRestoreEndpointAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.topology.TopologyEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.topology.TopologyEndpointAutoConfiguration;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsEndpointsTest.IN_TOPIC;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsEndpointsTest.OUT_TOPIC;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {IN_TOPIC, OUT_TOPIC})
class KafkaStreamsEndpointsTest {

    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "out";
    private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
    private static final String EXCEPTION_KEY = "exception";

    @Test
    @DisplayName(
            "Given normal operation, health's indicator should be fine"
    )
    void healthIndicatorUpTest() {
        final ApplicationContextRunner runner = setup("application-health-xyz");
        runner.run(context -> {
            final List<ProducerRecord<String, String>> records = List.of(
                    new ProducerRecord<>("in", "key", "value"),
                    new ProducerRecord<>("in", "key", "value")
            );
            receive(context, records, Status.UP, "out");
        });
    }

    @Test
    @DisplayName(
            "Given an exception, shutting down the client, health's indicator should be down"
    )
    void healthIndicatorDownTest() {
        ApplicationContextRunner runner = setup("application-health-abc");
        runner.run(context -> {
            final List<ProducerRecord<String, String>> records = List.of(
                    new ProducerRecord<>("in", "key", "value"),
                    new ProducerRecord<>("in", EXCEPTION_KEY, "value")
            );
            receive(context, records, Status.DOWN, "out");
        });
    }

    @Test
    void topologyEndpointTest() {
        ApplicationContextRunner runner = setupTopology();
        runner.run(context -> {
            final TopologyEndpoint endpoint = context.getBean(TopologyEndpoint.class);
            final String topology = endpoint.topology()
                                            .trim();
            assertThat(topology)
                    .isEqualTo("Topologies:\n" +
                                       "   Sub-topology: 0\n" +
                                       "    Source: in-consumer (topics: [in])\n" +
                                       "      --> filter\n" +
                                       "    Processor: filter (stores: [])\n" +
                                       "      --> in-store\n" +
                                       "      <-- in-consumer\n" +
                                       "    Processor: in-store (stores: [])\n" +
                                       "      --> in-store-stream\n" +
                                       "      <-- filter\n" +
                                       "    Processor: in-store-stream (stores: [])\n" +
                                       "      --> out-producer\n" +
                                       "      <-- in-store\n" +
                                       "    Sink: out-producer (topic: out)\n" +
                                       "      <-- in-store-stream");
        });
    }

    @Test
    void restorationsEndpointTest() {
        ApplicationContextRunner runner = setupStateRestore();
        runner.run(context -> assertThat(context).hasSingleBean(StateStoreRestoreEndpoint.class));
    }

    @Test
    void readonlystatestoreEndpointTest() {
        ApplicationContextRunner runner = setupReadOnlyStateStore();
        runner.run(context -> assertThat(context).hasSingleBean(ReadOnlyStateStoreEndpoint.class));
    }

    @Test
    void readonlystatestoreEndpointMissingTest() {
        ApplicationContextRunner runner = setupReadOnlyStateStoreWithoutServerConfig();
        runner.run(context -> assertThat(context).doesNotHaveBean(ReadOnlyStateStoreEndpoint.class));
    }

    private ApplicationContextRunner setup(String applicationId) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "management.endpoint.health.group.liveness.include=kStreams",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.application-id=" + applicationId,
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withConfiguration(
                        AutoConfigurations.of(
                                KafkaAutoConfiguration.class,
                                StreamBuilderFactoryConfiguration.class,
                                KafkaStreamsHealthIndicatorAutoConfiguration.class,
                                KStreamApplication.class
                        ));
    }

    private ApplicationContextRunner setupTopology() {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=topology",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.application-id=" + "application-topology-abc",
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withConfiguration(
                        AutoConfigurations.of(
                                KafkaAutoConfiguration.class,
                                StreamBuilderFactoryConfiguration.class,
                                KStreamApplication.class,
                                TopologyEndpointAutoConfiguration.class
                        ));
    }

    private ApplicationContextRunner setupStateRestore() {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=statestorerestore",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.application-id=" + "application-statestorerestore-abc",
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withConfiguration(
                        AutoConfigurations.of(
                                StateRestoreEndpointAutoConfiguration.class,
                                KafkaAutoConfiguration.class,
                                KafkaStreamsDefaultConfiguration.class,
                                StreamBuilderFactoryConfiguration.class
                                ));
    }

    private ApplicationContextRunner setupReadOnlyStateStore() {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=readonlystatestore",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.application.server=localhost:9090",
                        "spring.kafka.streams.properties.additional.serdes=org.apache.kafka.common.serialization.Serdes$LongSerde",
                        "spring.kafka.streams.application-id=" + "application-readonlystatestore-abc",
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withConfiguration(
                        AutoConfigurations.of(
                                InteractiveQueryEndpointAutoConfiguration.class,
                                KafkaAutoConfiguration.class,
                                KafkaStreamsDefaultConfiguration.class,
                                StreamBuilderFactoryConfiguration.class,
                                KStreamApplication.class
                        ));
    }

    private ApplicationContextRunner setupReadOnlyStateStoreWithoutServerConfig() {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=readonlystatestore",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.additional.serdes=org.apache.kafka.common.serialization.Serdes$LongSerde",
                        "spring.kafka.streams.application-id=" + "application-readonlystatestore-abc",
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withConfiguration(
                        AutoConfigurations.of(
                                InteractiveQueryEndpointAutoConfiguration.class,
                                KafkaAutoConfiguration.class,
                                KafkaStreamsDefaultConfiguration.class,
                                StreamBuilderFactoryConfiguration.class,
                                KStreamApplication.class
                        ));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void receive(ConfigurableApplicationContext context,
                         List<ProducerRecord<String, String>> records,
                         Status expected,
                         String... topics) throws Exception {

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-id0", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);

        try (Consumer<String, String> consumer = cf.createConsumer()) {
            KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);

            CountDownLatch latch = new CountDownLatch(records.size());
            for (ProducerRecord<String, String> record : records) {

                ListenableFuture<SendResult<String, String>> future = template.send(record);
                future.addCallback(
                        new ListenableFutureCallback<>() {

                            @Override
                            @SuppressWarnings("NullableProblems")
                            public void onFailure(Throwable ignored) {}

                            @Override
                            public void onSuccess(SendResult<String, String> result) {
                                latch.countDown();
                            }
                        });
            }

            latch.await(5, TimeUnit.SECONDS);
            embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
            KafkaTestUtils.getRecords(consumer, 1000);
            TimeUnit.SECONDS.sleep(5);
            checkHealth(context, expected);
        } finally {
            pf.destroy();
        }
    }

    private static void checkHealth(ConfigurableApplicationContext context, Status expected) throws Exception {
        KafkaStreamsHealthIndicator indicator = context
                .getBean("kStreamsHealthIndicator", KafkaStreamsHealthIndicator.class);

        Health health = indicator.health();
        while (waitFor(health.getStatus(), health.getDetails())) {
            TimeUnit.SECONDS.sleep(2);
            health = indicator.health();
        }
        assertThat(health.getStatus()).isEqualTo(expected);
    }

    private static boolean waitFor(Status status, Map<String, Object> details) {
        if (status == Status.UP) {
            String threadState = (String) details.get("threadState");
            return threadState != null
                    && (threadState.equalsIgnoreCase(KafkaStreams.State.REBALANCING.name())
                    || threadState.equalsIgnoreCase("PARTITIONS_REVOKED")
                    || threadState.equalsIgnoreCase("PARTITIONS_ASSIGNED")
                    || threadState.equalsIgnoreCase(
                    KafkaStreams.State.PENDING_SHUTDOWN.name()));
        }
        return false;
    }



    @Configuration
    @EnableKafkaStreams
    public static class StreamBuilderFactoryConfiguration {

        @Bean
        public KafkaStreamsConfiguration defaultKafkaStreamsConfig(ObjectProvider<KafkaProperties> propertiesObjectProvider) {
            final KafkaProperties properties = propertiesObjectProvider.getIfAvailable();
            if (properties == null) {
                return null;
            }
            return new KafkaStreamsConfiguration(properties.buildStreamsProperties());
        }

        @Bean
        public StreamsBuilderFactoryBeanConfigurer streamsUncaughtExceptionHandlerConfigurer() {
            return fb -> fb.setStreamsUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT);
        }

    }


    @Configuration
    public static class KStreamApplication {

        @Autowired
        public void stream(final ObjectProvider<StreamsBuilder> builderProvider) {

            final StreamsBuilder builder = builderProvider.getIfAvailable();
            if (builder == null) {
                return;
            }

            builder.<String, String>stream(IN_TOPIC, Consumed.as("in-consumer"))
                   .filter((key, value) -> {
                       if (key.equals(EXCEPTION_KEY)) {
                           throw new IllegalArgumentException();
                       }
                       return true;
                   }, Named.as("filter"))
                   .toTable(Named.as("in-store"))
                   .toStream(Named.as("in-store-stream"))
                   .to(OUT_TOPIC, Produced.as("out-producer"));
        }
    }
}