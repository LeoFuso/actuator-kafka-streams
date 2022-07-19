package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup.StreamBuilderFactoryConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils.KafkaStreamTestUtils;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"in", "out"})
class KafkaStreamsHealthIndicatorTest {

    private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

    private static final String EXCEPTION_KEY = "exception";

    @Test
    @DisplayName("Given enabled indicator, when App finishes starting, then bean should be found")
    void th0() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(true);
        /* When & Then */
        runner.run(context -> {
            assertThat(context).hasSingleBean(KafkaStreamsHealthIndicator.class);
            context.close();
        });
    }

    @Test
    @DisplayName("Given disabled indicator, when App finishes starting, then bean should not be found")
    void th1() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(false);
        /* When & Then */
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(KafkaStreamsHealthIndicator.class);
            context.close();
        });
    }


    @Test
    @DisplayName("Given normal op, no records, when asked for HC, then should return Up")
    void th2() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(true);
        runner.run(context -> {

            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);

            /* When & Then */
            checkHealth(indicator, Status.UP);
            context.close();
        });
    }

    @Test
    @DisplayName("Given normal op, some records, when asked for HC, then should return Up")
    void th3() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(true);
        runner.run(context -> {

            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);

            /* When */
            KafkaStreamTestUtils.receive(
                    new ProducerRecord<>("in", "key", "some-value"),
                    new ProducerRecord<>("in", "key", "other-value")
            );
            /* Then */
            checkHealth(indicator, Status.UP);
            context.registerShutdownHook();

        });
    }

    @Test
    @DisplayName("Given faulty record and shutdown client handling, when asked for HC, then should return Down")
    void th4() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(true);
        runner.withBean(
                StreamsBuilderFactoryBeanCustomizer.class,
                () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT)
        );
        runner.run(context -> {

            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);

            /* When */
            KafkaStreamTestUtils.receive(
                    new ProducerRecord<>("in", "key", "some-value"),
                    new ProducerRecord<>("in", EXCEPTION_KEY, "exception-value"),
                    new ProducerRecord<>("in", "key", "other-value")
            );
            /* Then */
            checkHealth(indicator, Status.DOWN);
            context.registerShutdownHook();

        });
    }

    @Test
    @DisplayName("Given faulty record, and shutdown app handling, when asked for HC, then should return Down")
    void th5() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(true);
        runner.withBean(
                StreamsBuilderFactoryBeanCustomizer.class,
                () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION)
        );

        runner.run(context -> {

            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);

            /* When */
            KafkaStreamTestUtils.receive(
                    new ProducerRecord<>("in", "key", "some-value"),
                    new ProducerRecord<>("in", EXCEPTION_KEY, "exception-value"),
                    new ProducerRecord<>("in", "key", "other-value")
            );
            /* Then */
            checkHealth(indicator, Status.DOWN);
            context.registerShutdownHook();

        });
    }

    @Test
    @DisplayName("Given faulty record, and replace thread handling, when asked for HC, then should return Down")
    void th6() {
        /* Given */
        ApplicationContextRunner runner = healthCheck(true);
        runner.withBean(
                StreamsBuilderFactoryBeanCustomizer.class,
                () -> fb -> fb.setStreamsUncaughtExceptionHandler(exception -> REPLACE_THREAD)
        );

        runner.run(context -> {

            final KafkaStreamsHealthIndicator indicator = context.getBean(KafkaStreamsHealthIndicator.class);

            /* When */
            KafkaStreamTestUtils.receive(
                    new ProducerRecord<>("in", "key", "some-value"),
                    new ProducerRecord<>("in", EXCEPTION_KEY, "exception-value"),
                    new ProducerRecord<>("in", "key", "other-value")
            );
            /* Then */
            checkHealth(indicator, Status.UP);
            context.registerShutdownHook();
        });
    }


    private ApplicationContextRunner healthCheck(Boolean enabled) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "server.port=0",
                        "spring.jmx.enabled=false",
                        "management.endpoint.health.group.liveness.include=kStreams",
                        "spring.kafka.streams.properties.commit.interval.ms=1000",
                        "management.health.kStreams.enabled=" + enabled.toString(),
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.application-id=application-healthcheck",
                        "spring.kafka.bootstrap-servers=" + embeddedKafka.getBrokersAsString()
                )
                .withUserConfiguration(StreamBuilderFactoryConfiguration.class, KStreamApplication.class)
                .withConfiguration(
                        AutoConfigurations.of(
                                KafkaAutoConfiguration.class,
                                KafkaStreamsHealthIndicatorAutoConfiguration.class
                        ));
    }

    private static void checkHealth(KafkaStreamsHealthIndicator indicator, Status expected) throws Exception {
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
    public static class KStreamApplication {

        @Autowired
        public void stream(final ObjectProvider<StreamsBuilder> builderProvider) {

            final StreamsBuilder builder = builderProvider.getIfAvailable();
            if (builder == null) {
                return;
            }

            builder.<String, String>stream("in", Consumed.as("in-consumer"))
                   .filter((key, value) -> {
                       if (key.equals(EXCEPTION_KEY)) {
                           throw new IllegalArgumentException();
                       }
                       return true;
                   }, Named.as("exception-filter"))
                   .to("out", Produced.as("out-sink"));
        }
    }
}