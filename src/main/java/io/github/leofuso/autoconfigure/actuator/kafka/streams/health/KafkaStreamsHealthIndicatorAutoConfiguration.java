package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsHealthIndicatorAutoConfiguration.INDICATOR;

@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnEnabledHealthIndicator(INDICATOR)
@AutoConfigureAfter({KafkaStreamsDefaultConfiguration.class})
public class KafkaStreamsHealthIndicatorAutoConfiguration {

    public static final String INDICATOR = "kStreams";

    @Bean
    public KafkaStreamsHealthIndicator kStreamsHealthIndicator(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider) {
        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        if (factory != null) {
            return new KafkaStreamsHealthIndicator(factory);
        }
        return null;
    }

}
