package io.github.leofuso.autoconfigure.actuator.kafka.streams.health.setup;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

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

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsUncaughtExceptionHandlerConfigurer() {
        return fb -> fb.setStreamsUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT);
    }

}
