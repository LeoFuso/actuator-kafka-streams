package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.time.Duration;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup.StreamBuilderFactoryConfiguration;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils.addRandomTopic;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils.expect;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils.produce;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"out"},
               partitions = 3,
               brokerProperties = {
                       "group.min.session.timeout.ms=10",
               }
)
@TestMethodOrder(MethodOrderer.MethodName.class)
class KafkaStreamsHealthIndicatorTest {

    private final EmbeddedKafkaBroker broker;

    KafkaStreamsHealthIndicatorTest(final EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    private static final String ILLEGAL_ARG_EXP_KEY = "ILLEGAL_ARGUMENT";
    private static final String NPE_ARG_EXP_KEY = "NULL_POINTER";

    @Test
    @DisplayName("Given enabled indicator, when App finishes starting, then bean should be found")
    void th0() {
        /* Given, When & Then */
        healthCheck(true)
                .run(context -> assertThat(context).hasSingleBean(KafkaStreamsHealthIndicator.class));
    }

    @Test
    @DisplayName("Given disabled indicator, when App finishes starting, then bean should not be found")
    void th1() {
        /* Given, When & Then */
        healthCheck(false)
                .run(context -> assertThat(context).doesNotHaveBean(KafkaStreamsHealthIndicator.class));
    }

    @Test
    @DisplayName("Given normal op, no records, when asked for HC, then should return Up")
    void th2() {
        /* Given */
        healthCheck(true).run(context -> {
            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
            /* When & Then */
            expect(indicator, Status.UP);
        });
    }


    @Test
    @DisplayName("Given normal op, some records, when asked for HC, then should return Up")
    void th3() {
        /* Given */

        final String topic = addRandomTopic(broker);
        healthCheck(true).run(context -> {
            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
            produce(
                    broker,
                    new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                    new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value")
            );
            /* When & Then */
            expect(indicator, Status.UP);
        });
    }

    @Test
    @DisplayName("Given faulty record (NPE) and shutdown client handling, when asked for HC, then should return Down")
    void th4() {
        /* Given */
        final String topic = addRandomTopic(broker);
        healthCheck(true)
                .withBean(
                        StreamsBuilderFactoryBeanCustomizer.class,
                        () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT)
                )
                .run(context -> {
                    final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
                    expect(indicator, Status.UP);
                    produce(
                            broker,
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, NPE_ARG_EXP_KEY, "exception-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value")
                    );
                    /* When & Then */
                    expect(indicator, Status.DOWN);
                });
    }

    @Test
    @DisplayName("Given faulty record(NPE), and shutdown app handling, when asked for HC, then should return Down")
    void th5() {
        /* Given */
        final String topic = addRandomTopic(broker);
        healthCheck(true)
                .withBean(
                        StreamsBuilderFactoryBeanCustomizer.class,
                        () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION)
                )
                .run(context -> {
                    final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
                    expect(indicator, Status.UP);
                    produce(
                            broker,
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, NPE_ARG_EXP_KEY, "exception-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value")
                    );
                    /* When & Then */
                    expect(indicator, Status.DOWN, Duration.ofSeconds(3));
                });
    }

    @Test
    @DisplayName("Given faulty record(NPE), and replace thread handling, when asked for HC, then should return Up")
    void th6() {
        /* Given */
        final String topic = addRandomTopic(broker);
        healthCheck(true)
                .withBean(
                        StreamsBuilderFactoryBeanCustomizer.class,
                        () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> REPLACE_THREAD)
                )
                .run(context -> {
                    final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
                    expect(indicator, Status.UP);

                    produce(
                            broker,
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, NPE_ARG_EXP_KEY, "exception-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value")
                    );
                    produce(
                            broker,
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-other-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "and-other-value")
                    );
                    /* When & Then */
                    expect(indicator, Status.UP, Duration.ofSeconds(5));
                });
    }

    @Test
    @DisplayName("Given faulty record(ILLEGAL), and replace thread handling, when asked for HC, then should return Down")
    void th7() {
        /* Given */
        final String topic = addRandomTopic(broker);
        healthCheck(true)
                .withBean(
                        StreamsBuilderFactoryBeanCustomizer.class,
                        () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> REPLACE_THREAD)
                )
                .run(context -> {
                    final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
                    expect(indicator, Status.UP);

                    produce(
                            broker,
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, ILLEGAL_ARG_EXP_KEY, "exception-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "other-value")
                    );
                    /* When & Then */
                    expect(indicator, Status.DOWN);
                });
    }

    @Test
    @DisplayName("Given faulty record(NPE), no strategy, when asked for HC, then should return Up")
    void th8() {
        /* Given */
        final String topic = addRandomTopic(broker);
        healthCheck(true)
                .run(context -> {
                    final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);
                    expect(indicator, Status.UP);
                    produce(
                            broker,
                            new ProducerRecord<>(topic, 0, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, 1, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, 2, String.valueOf(UUID.randomUUID()), "some-value"),
                            new ProducerRecord<>(topic, 0, NPE_ARG_EXP_KEY, "exception-value"),
                            new ProducerRecord<>(topic, 1, String.valueOf(UUID.randomUUID()), "other-value"),
                            new ProducerRecord<>(topic, 2, String.valueOf(UUID.randomUUID()), "other-value")
                    );
                    /* When & Then */
                    expect(indicator, Status.UP, Duration.ofSeconds(5));
                });
    }



    private ApplicationContextRunner healthCheck(Boolean enabled) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "log4j.category.org.apache.kafka.clients=OFF",
                        "log4j.category.org.apache.kafka.common.network.Selector=OFF",
                        "log4j.category.kafka.server.ReplicaFetcherThread=OFF",
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoint.health.group.liveness.include=kStreams",
                        "management.health.kStreams.enabled=" + enabled.toString(),
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        /*
                         * This config prevents the Broker to enter a re-balance state between test runs, causing the test thread to hang,
                         * behaving almost like a fork bomb. Wasted two days of my life trying to solve this issue. Fun.
                         */
                        "spring.kafka.streams.application-id=application-" + UUID.randomUUID(),
                        "spring.kafka.streams.cleanup.on-startup=true",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.auto.offset.reset=earliest",
                        "spring.kafka.streams.properties.commit.interval.ms=100",
                        "spring.kafka.streams.properties.num.stream.threads=3",
                        "spring.kafka.streams.properties.session.timeout.ms=100",
                        "spring.kafka.streams.properties.heartbeat.interval.ms=50",
                        "spring.kafka.streams.properties.fetch.max.wait.ms=60"
                        //"spring.kafka.streams.properties.metadata.max.age.ms=10"
                )
                .withUserConfiguration(StreamBuilderFactoryConfiguration.class, KStreamApplication.class)
                .withConfiguration(
                        AutoConfigurations.of(
                                KafkaAutoConfiguration.class,
                                KafkaStreamsHealthIndicatorAutoConfiguration.class
                        ));
    }

    @Configuration
    public static class KStreamApplication {

        @Autowired
        public void stream(final ObjectProvider<StreamsBuilder> builderProvider) {

            final StreamsBuilder builder = builderProvider.getIfAvailable();
            if (builder == null) {
                return;
            }

            final Pattern pattern = Pattern.compile(".*");
            builder.<String, String>stream(pattern, Consumed.as("in-consumer"))
                   .filter((key, value) -> {
                       if (key.equalsIgnoreCase(NPE_ARG_EXP_KEY)) {
                           throw new NullPointerException();
                       }
                       if (key.equalsIgnoreCase(ILLEGAL_ARG_EXP_KEY)) {
                           throw new IllegalArgumentException();
                       }
                       return true;
                   }, Named.as("exception-filter"))
                   .to("out", Produced.as("out-sink"));
        }
    }
}