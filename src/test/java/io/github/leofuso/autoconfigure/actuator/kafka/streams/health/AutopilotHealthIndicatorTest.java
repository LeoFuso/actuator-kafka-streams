package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.time.Duration;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotHealthIndicator;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotThreadEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup.StreamBuilderFactoryConfiguration;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils.addRandomTopic;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils.expect;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils.produce;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"out"},
               partitions = 3,
               brokerProperties = {
                       "group.min.session.timeout.ms=10",
               }
)
@TestMethodOrder(MethodOrderer.MethodName.class)
class AutopilotHealthIndicatorTest {

    private final EmbeddedKafkaBroker broker;

    AutopilotHealthIndicatorTest(final EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    private static final String SIMULATE_HIGH_LOAD = "HIGH_LOAD";

    @Test
    @DisplayName("Given enabled autopilot, when App finishes starting, then bean should be found")
    void t0() {
        /* Given, When & Then */
        autopilot(true)
                .run(context -> {
                    assertThat(context).hasSingleBean(AutopilotHealthIndicator.class);
                    assertThat(context).hasSingleBean(AutopilotThreadEndpoint.class);
                });
    }

    @Test
    @DisplayName("Given disabled autopilot, when App finishes starting, then bean should not be found")
    void t1() {
        /* Given, When & Then */
        autopilot(false)
                .run(context -> {
                    assertThat(context).doesNotHaveBean(AutopilotHealthIndicator.class);
                    assertThat(context).doesNotHaveBean(AutopilotThreadEndpoint.class);
                });
    }

    @Test
    @DisplayName("Given normal op, no records, when asked for the autopilot, then should return Up")
    void t2() {
        /* Given */
        autopilot(true).run(context -> {
            final AutopilotHealthIndicator indicator = context.getBean(AutopilotHealthIndicator.class);
            /* When & Then */
            expect(indicator, Status.UP);
        });
    }


    @Test
    @DisplayName("Given normal op, some records, when asked for the autopilot, then should return Up")
    void t3() {

        /* Given */
        final String topic = addRandomTopic(broker);
        autopilot(true).run(context -> {
            final AutopilotHealthIndicator indicator = context.getBean(AutopilotHealthIndicator.class);
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
    @Timeout(15)
    @DisplayName("Given troublesome high-load, when asked for the autopilot, then should return Boost")
    void t4() {

        /* Given */
        final String topic = addRandomTopic(broker);
        autopilot(true)
                .run(context -> {
                    final AutopilotHealthIndicator indicator = context.getBean(AutopilotHealthIndicator.class);
                    expect(indicator, Status.UP);
                    produce(
                            broker,
                            new ProducerRecord<>(topic, SIMULATE_HIGH_LOAD, "exception-value"),
                            new ProducerRecord<>(topic, SIMULATE_HIGH_LOAD, "some-value-1"),
                            new ProducerRecord<>(topic, SIMULATE_HIGH_LOAD, "some-value-2"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value-3"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value-4"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value-5"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value-6"),
                            new ProducerRecord<>(topic, String.valueOf(UUID.randomUUID()), "some-value-7")
                    );
                    /* When & Then */
                    expect(indicator, AutopilotHealthIndicator.BOOST, Duration.ofSeconds(2));
                });
    }

    @Test
    @Timeout(5)
    @Disabled("Only works alone. Hanging indefinitely.")
    @DisplayName("Given normal op, no record, and with additional threads, when asked for the autopilot, then should return Nerf")
    void t5() {

        /* Given */
        autopilot(true)
                .run(context -> {
                    final AutopilotHealthIndicator indicator = context.getBean(AutopilotHealthIndicator.class);

                    final AutopilotThreadEndpoint autopilotThread = context.getBean(AutopilotThreadEndpoint.class);
                    autopilotThread.addStreamThread();

                    /* When & Then */
                    expect(indicator, AutopilotHealthIndicator.NERF);
                });
    }

    private ApplicationContextRunner autopilot(Boolean enabled) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.health.autopilot.enabled=" + enabled,
                        "management.endpoints.web.exposure.include=" + (enabled ? "autopilotthread" : ""),
                        "management.health.autopilot.stream-thread-limit=5",
                        "management.health.autopilot.lag-threshold=5",
                        "management.health.autopilot.period=5s",
                        "management.health.autopilot.timeout=600ms",
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "spring.kafka.streams.application-id=application-" + UUID.randomUUID(),
                        "spring.kafka.streams.cleanup.on-startup=true",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.auto.offset.reset=earliest",
                        "spring.kafka.streams.properties.commit.interval.ms=100",
                        "spring.kafka.streams.properties.num.stream.threads=1",
                        "spring.kafka.streams.properties.session.timeout.ms=20000",
                        "spring.kafka.streams.properties.heartbeat.interval.ms=15000",
                        "spring.kafka.streams.properties.fetch.max.wait.ms=100"
                )
                .withUserConfiguration(StreamBuilderFactoryConfiguration.class, KStreamApplication.class)
                .withConfiguration(
                        AutoConfigurations.of(
                                KafkaAutoConfiguration.class,
                                AutopilotAutoConfiguration.class
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
                       if (key.equalsIgnoreCase(SIMULATE_HIGH_LOAD)) {
                           try {
                               Thread.sleep(10_000);
                           } catch (InterruptedException ignored) { /* ignored */ }
                       }
                       return true;
                   }, Named.as("high-load-filter"))
                   .to("out", Produced.as("out-sink"));
        }
    }
}