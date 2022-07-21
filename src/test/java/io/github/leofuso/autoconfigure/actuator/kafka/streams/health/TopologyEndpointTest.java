package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.ResourceUtils;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup.StreamBuilderFactoryConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.topology.TopologyEndpoint;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.topology.TopologyEndpointAutoConfiguration;

import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka
@TestMethodOrder(MethodOrderer.MethodName.class)
class TopologyEndpointTest {

    private final EmbeddedKafkaBroker broker;

    TopologyEndpointTest(final EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    @Test
    @DisplayName("Given enabled topology, when App finishes starting, then bean should be found")
    void th0() {
        /* Given, When & Then */
        topology(true)
                .run(context -> assertThat(context).hasSingleBean(TopologyEndpoint.class));
    }

    @Test
    @DisplayName("Given disabled topology, when App finishes starting, then bean should not be found")
    void th1() {
        /* Given, When & Then */
        topology(false)
                .run(context -> assertThat(context).doesNotHaveBean(TopologyEndpoint.class));
    }

    @Test
    @DisplayName("Given a topology, when asked for it, then return it.")
    void th2() {
        /* Given */
        topology(true).run(context -> {
            final TopologyEndpoint topology = context.getBean(TopologyEndpoint.class);
            /* When & Then */
            final String actual = topology.topology()
                                          .trim();

            File file = ResourceUtils.getFile("classpath:topology.txt");
            final String lineSeparator = System.getProperty("line.separator");
            final List<String> lines = Files.readAllLines(file.toPath());
            final String[] actualSplit = actual.split(lineSeparator);
            for (int i = 0; i < lines.size(); i++) {
                final String expectedLine = lines.get(i);
                assertThat(expectedLine)
                        .contains(actualSplit[i]);
            }
        });
    }



    private ApplicationContextRunner topology(Boolean enabled) {
        return new ApplicationContextRunner()
                .withPropertyValues(
                        "logging.level.org.apache.kafka=OFF",
                        "management.endpoints.web.exposure.include=" + (enabled ? "topology" : ""),
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
                                TopologyEndpointAutoConfiguration.class
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