package io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

/**
 * Tests stand-in replacement.
 */
@Configuration
@EnableKafkaStreams
public class StreamBuilderFactoryConfiguration {

    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(ObjectProvider<KafkaProperties> propertiesObjectProvider) {
        final KafkaProperties properties = propertiesObjectProvider.getIfAvailable();
        if (properties == null) {
            return null;
        }
        return new KafkaStreamsConfiguration(properties.buildStreamsProperties());
    }
}
