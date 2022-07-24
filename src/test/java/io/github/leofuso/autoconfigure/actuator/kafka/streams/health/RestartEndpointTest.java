package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup.StreamBuilderFactoryConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.restart.RestartEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.restart.RestartEndpointAutoConfiguration;

import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka
@TestMethodOrder(MethodOrderer.MethodName.class)
class RestartEndpointTest {

    private final EmbeddedKafkaBroker broker;

    RestartEndpointTest(final EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    @Test
    @DisplayName("Given enabled restart, when App finishes starting, then bean should be found")
    void t0() {
        /* Given, When & Then */
        restart(true)
                .run(context -> assertThat(context).hasSingleBean(RestartEndpoint.class));
    }

    @Test
    @DisplayName("Given disabled restart, when App finishes starting, then bean should not be found")
    void t1() {
        /* Given, When & Then */
        restart(false)
                .run(context -> assertThat(context).doesNotHaveBean(RestartEndpoint.class));
    }

    @Test
    @DisplayName("Given a restart, when asked for it, then should have restarted.")
    void t2() {

        /* Given */
        restart(true).run(context -> {

            final RestartEndpoint restart = context.getBean(RestartEndpoint.class);

            /* When */
            restart.restart();

            final StreamsBuilderFactoryBean factory = context.getBean(StreamsBuilderFactoryBean.class);
            assertThat(factory)
                    .isNotNull()
                    .extracting(StreamsBuilderFactoryBean::isRunning)
                    .isSameAs(true);

        });
    }



    private ApplicationContextRunner restart(Boolean enabled) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=" + (enabled ? "restart" : ""),
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "spring.kafka.streams.application-id=application-" + UUID.randomUUID(),
                        "spring.kafka.streams.cleanup.on-startup=true",
                        "spring.kafka.streams.properties.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "spring.kafka.streams.properties.auto.offset.reset=earliest"
                )
                .withUserConfiguration(StreamBuilderFactoryConfiguration.class, KStreamApplication.class)
                .withConfiguration(
                        AutoConfigurations.of(
                                KafkaAutoConfiguration.class,
                                RestartEndpointAutoConfiguration.class
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

            builder.<String, String>stream("out-in", Consumed.as("out-consumer"))
                   .transform(() -> new Transformer<String, String, KeyValue<UUID, Integer>>() {
                       @Override
                       public void init(final ProcessorContext context) {}

                       @Override
                       public KeyValue<UUID, Integer> transform(final String key, final String value) {
                           return new KeyValue<>(UUID.fromString(key), Integer.valueOf(value));
                       }

                       @Override
                       public void close() {}
                   }, Named.as("out-to-uuid"))
                   .groupByKey(Grouped.<UUID, Integer>as("out-group-by")
                                      .withKeySerde(Serdes.UUID())
                                      .withValueSerde(Integer()))
                   .reduce(
                           Integer::sum,
                           Named.as("out"),
                           Materialized.<UUID, Integer, KeyValueStore<Bytes, byte[]>>as("out-store")
                                       .withKeySerde(Serdes.UUID())
                                       .withValueSerde(Serdes.Integer())
                   )
                   .toStream(Named.as("out-as-stream"))
                   .transform(() -> new Transformer<UUID, Integer, KeyValue<String, String>>() {
                       @Override
                       public void init(final ProcessorContext context) {}

                       @Override
                       public KeyValue<String, String> transform(final UUID key, final Integer value) {
                           return new KeyValue<>(key.toString(), value.toString());
                       }

                       @Override
                       public void close() {}
                   }, Named.as("out-to-string"))
                   .to("out", Produced.as("out-sink"));
        }
    }
}