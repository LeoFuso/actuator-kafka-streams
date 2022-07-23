package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KafkaStreamsHealthIndicatorAutoConfiguration.INDICATOR;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;

@AutoConfiguration(after = {KafkaStreamsDefaultConfiguration.class}, before = {HealthEndpointAutoConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnEnabledHealthIndicator(INDICATOR)
@EnableConfigurationProperties(KafkaStreamHealthIndicatorProperties.class)
public class KafkaStreamsHealthIndicatorAutoConfiguration {

    public static final String INDICATOR = "kstreams";

    private final KafkaStreamHealthIndicatorProperties properties;

    public KafkaStreamsHealthIndicatorAutoConfiguration(final KafkaStreamHealthIndicatorProperties properties) {
        this.properties = properties;
    }

    @Bean
    @DependsOn({DEFAULT_STREAMS_BUILDER_BEAN_NAME})
    @ConditionalOnMissingBean(name = "kstreamsHealthIndicator")
    public KafkaStreamsHealthIndicator kstreamsHealthIndicator(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider) {
        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        if (factory != null) {
            return new KafkaStreamsHealthIndicator(
                    factory,
                    !properties.isAllowThreadLoss(),
                    properties.getMinimumNumberOfLiveStreamThreads()
            );
        }
        return null;
    }

}
