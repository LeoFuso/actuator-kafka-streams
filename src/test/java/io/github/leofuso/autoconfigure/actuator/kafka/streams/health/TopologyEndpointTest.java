package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
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

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"in", "out"})
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
    @DisplayName("Given a simple topology, when asked for it, then return it.")
    void th2() {
        /* Given */
        topology(true).run(context -> {
            final TopologyEndpoint topology = context.getBean(TopologyEndpoint.class);
            /* When & Then */
            final String actual = topology.topology();

            File file = ResourceUtils.getFile("classpath:topology.txt");

            /* Files.writeString(file.toPath(), actualTopology, StandardCharsets.UTF_8) */

            final String expectedTopology = Files.readString(file.toPath());
            assertThat(actual)
                    .isEqualTo(expectedTopology);
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

            final Pattern pattern = Pattern.compile(".*");
            builder.<String, String>stream(pattern, Consumed.as("in-consumer"))
                   .to("out", Produced.as("out-sink"));
        }
    }
}