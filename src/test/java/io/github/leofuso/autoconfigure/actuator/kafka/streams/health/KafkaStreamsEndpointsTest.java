package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;


import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Disabled;
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
import org.springframework.context.support.ConversionServiceFactoryBean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.ResourceUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.CompositeStateAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint.InteractiveQueryEndpointAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint.ReadOnlyStateStoreEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore.StateRestoreEndpointAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore.StateStoreRestoreEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.topology.TopologyEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.topology.TopologyEndpointAutoConfiguration;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsEndpointsTest.IN_TOPIC;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsEndpointsTest.OUT_TOPIC;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsEndpointsTest.SUM_IN_TOPIC;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsEndpointsTest.SUM_OUT_TOPIC;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.UUID;
import static org.apache.kafka.common.serialization.Serdes.UUIDSerde;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {IN_TOPIC, OUT_TOPIC, SUM_IN_TOPIC, SUM_OUT_TOPIC})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class KafkaStreamsEndpointsTest {

    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "out";
    public static final String SUM_IN_TOPIC = "sum-in";
    public static final String SUM_OUT_TOPIC = "sum-out";

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
    @Disabled
    @DisplayName(
            "Given a topology, return it"
    )
    void topologyEndpointTest() {
        ApplicationContextRunner runner = setupTopology();
        runner.run(context -> {
            final TopologyEndpoint endpoint = context.getBean(TopologyEndpoint.class);
            final String actualTopology = endpoint.topology();

            File file = ResourceUtils.getFile("classpath:topology.txt");

            /* Files.writeString(file.toPath(), actualTopology, StandardCharsets.UTF_8) */

            final String expectedTopology = Files.readString(file.toPath());
            assertThat(actualTopology)
                    .isEqualTo(expectedTopology);
        });
    }

    @Test
    @Disabled
    void restorationsEndpointTest() {
        ApplicationContextRunner runner = setupStateRestore();
        runner.run(context -> assertThat(context).hasSingleBean(StateStoreRestoreEndpoint.class));
    }

    @Test
    @DisplayName(
            "Given all necessary configurations, readonlystatestore endpoint should be present"
    )
    void readonlystatestoreEndpointTest() {
        ApplicationContextRunner runner = setupReadOnlyStateStore(9090);
        runner.run(context -> assertThat(context).hasSingleBean(ReadOnlyStateStoreEndpoint.class));
    }

    @Test
    @DisplayName(
            "Given configurations, but missing [application.server] config, readonlystatestore endpoint should be absent"
    )
    void readonlystatestoreEndpointMissingTest() {
        ApplicationContextRunner runner = setupReadOnlyStateStoreWithoutServerConfig();
        runner.run(context -> assertThat(context).doesNotHaveBean(ReadOnlyStateStoreEndpoint.class));
    }

    @Test
    @Disabled
    @DisplayName(
            "Given a state, readonlystatestore should return correct value, locally"
    )
    void readonlystatestoreLocallyEndpointTest() {
        ApplicationContextRunner runner = setupReadOnlyStateStore(9090);
        runner.run(context -> {
            assertThat(context).hasSingleBean(ReadOnlyStateStoreEndpoint.class);
            final ReadOnlyStateStoreEndpoint endpoint = context.getBean(ReadOnlyStateStoreEndpoint.class);

            final String key = "adde3d47-ee2f-4e3a-9fa0-1ab274ad1ee4";
            final List<ProducerRecord<String, String>> records = List.of(
                    new ProducerRecord<>(SUM_IN_TOPIC, key, "1"),
                    new ProducerRecord<>(SUM_IN_TOPIC, key, "2")
            );

            receive(records, SUM_OUT_TOPIC);
            final Map<String, String> response =
                    endpoint.find("sum-store", key, UUIDSerde.class.getName());

            assertThat(response)
                    .isNotEmpty()
                    .containsExactly(Map.entry(key, "3"));

        });
    }

    @Test
    @Disabled
    @DisplayName(
            "Given a state, readonlystatestore should return correct value, remotely"
    )
    void readonlystatestoreRemotelyEndpointTest() {

        ApplicationContextRunner server = setupReadOnlyStateStore(9990);
        server.run(serverContext -> {

            final Set<String> keys = IntStream
                    .range(0, 3)
                    .mapToObj(ignored -> UUID.randomUUID())
                    .map(UUID::toString)
                    .collect(Collectors.toSet());

            ApplicationContextRunner client = setupReadOnlyStateStore(9991);
            client.run(clientContext -> {

                for (String key : keys) {

                    final List<ProducerRecord<String, String>> records = List.of(
                            new ProducerRecord<>(SUM_IN_TOPIC, key, "1"),
                            new ProducerRecord<>(SUM_IN_TOPIC, key, "2")
                    );

                    receive(records, SUM_OUT_TOPIC);

                    final ReadOnlyStateStoreEndpoint clientEndpoint =
                            clientContext.getBean(ReadOnlyStateStoreEndpoint.class);

                    final Map<String, String> response =
                            clientEndpoint.find("sum-store", key, UUIDSerde.class.getName());

                    assertThat(response)
                            .isNotEmpty()
                            .containsExactly(Map.entry(key, "3"));
                }

                for (String key : keys) {

                    final ReadOnlyStateStoreEndpoint serverEndpoint =
                            serverContext.getBean(ReadOnlyStateStoreEndpoint.class);

                    final Map<String, String> response =
                            serverEndpoint.find("sum-store", key, UUIDSerde.class.getName());

                    assertThat(response)
                            .isNotEmpty()
                            .containsExactly(Map.entry(key, "3"));
                }
            });
        });
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

    private ApplicationContextRunner setupReadOnlyStateStore(int port) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=readonlystatestore",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "spring.kafka.streams.cleanup.on-startup=true",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "spring.kafka.streams.properties.num.stream.threads=1",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.application.server=localhost:" + port,
                        "spring.kafka.streams.properties.additional.serdes=org.apache.kafka.common.serialization.Serdes$LongSerde",
                        "spring.kafka.streams.application-id=application-readonlystatestore-abc",
                        "spring.kafka.streams.properties.state.dir=./local-state-store/" + port,
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withConfiguration(
                        AutoConfigurations.of(
                                CompositeStateAutoConfiguration.class,
                                InteractiveQueryEndpointAutoConfiguration.class,
                                KafkaAutoConfiguration.class,
                                KafkaStreamsDefaultConfiguration.class,
                                StreamBuilderFactoryConfiguration.class,
                                KStreamApplication.class
                        ));
    }

    private void receive(ConfigurableApplicationContext context,
                         List<ProducerRecord<String, String>> records,
                         Status expected,
                         String... topics) throws Exception {
        receive(records, topics);
        checkHealth(context, expected);
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void receive(List<ProducerRecord<String, String>> records, String... topics) throws Exception {

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-id0", "false", embeddedKafka);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
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

            latch.await(2, TimeUnit.SECONDS);
            embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
            TimeUnit.SECONDS.sleep(2);
            KafkaTestUtils.getRecords(consumer, 1000);
        } finally {
            pf.destroy();
        }
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
                        "spring.kafka.streams.properties.additional.serdes=org.apache.kafka.common.serialization.Serdes.LongSerde",
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

   // @Configuration
   // @EnableKafkaStreams
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

        @Bean
        public ConversionServiceFactoryBean conversionService() {
            return new ConversionServiceFactoryBean();
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
                   }, Named.as("exception-filter"))
                   .to(OUT_TOPIC, Produced.as("out-sink"));


            builder.<String, String>stream(SUM_IN_TOPIC, Consumed.as("sum-consumer"))
                   .transform(() -> new Transformer<String, String, KeyValue<UUID, Integer>>() {
                       @Override
                       public void init(final ProcessorContext context) {}

                       @Override
                       public KeyValue<UUID, Integer> transform(final String key, final String value) {
                           return new KeyValue<>(UUID.fromString(key), Integer.valueOf(value));
                       }

                       @Override
                       public void close() {}
                   }, Named.as("sum-to-uuid"))
                   .groupByKey(Grouped.<UUID, Integer>as("sum-group-by")
                                      .withKeySerde(UUID())
                                      .withValueSerde(Integer()))
                   .reduce(
                           Integer::sum,
                           Named.as("sum"),
                           Materialized.<UUID, Integer, KeyValueStore<Bytes, byte[]>>as("sum-store")
                                       .withKeySerde(UUID())
                                       .withValueSerde(Serdes.Integer())
                   )
                   .toStream(Named.as("sum-as-stream"))
                   .transform(() -> new Transformer<UUID, Integer, KeyValue<String, String>>() {
                       @Override
                       public void init(final ProcessorContext context) {}

                       @Override
                       public KeyValue<String, String> transform(final UUID key, final Integer value) {
                           return new KeyValue<>(key.toString(), value.toString());
                       }

                       @Override
                       public void close() {}
                   }, Named.as("sum-to-string"))
                   .to(SUM_OUT_TOPIC, Produced.as("sum-sink"));
        }
    }
}