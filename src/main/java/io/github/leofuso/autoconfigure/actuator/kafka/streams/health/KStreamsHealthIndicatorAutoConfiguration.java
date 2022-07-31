package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import java.util.Objects;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.health.KStreamsHealthIndicatorAutoConfiguration.INDICATOR;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for {@link KStreamsHealthIndicator}.
 */
@AutoConfiguration(
        after = {KafkaStreamsDefaultConfiguration.class},
        before = {HealthEndpointAutoConfiguration.class}
)
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnEnabledHealthIndicator(INDICATOR)
@EnableConfigurationProperties(KStreamsIndicatorProperties.class)
public class KStreamsHealthIndicatorAutoConfiguration {

    /**
     * Global reference for the {@link KStreamsHealthIndicator indicator}.
     */
    public static final String INDICATOR = "kstreams";

    /**
     * Properties file.
     */
    private final KStreamsIndicatorProperties properties;

    /**
     * Creates a new KafkaStreamsHealthIndicatorAutoConfiguration instance.
     *
     * @param properties used to configure the {@link KStreamsHealthIndicator indicator}.
     */
    public KStreamsHealthIndicatorAutoConfiguration(final KStreamsIndicatorProperties properties) {
        this.properties = Objects.requireNonNull(properties, "Missing required properties.");
    }

    /**
     * Main bean factory for the {@link KStreamsHealthIndicator}.
     *
     * @param provider used to create a {@link KStreamsHealthIndicator}.
     * @return a new {@link KStreamsHealthIndicator}.
     */
    @Bean
    @DependsOn({DEFAULT_STREAMS_BUILDER_BEAN_NAME})
    @ConditionalOnMissingBean(name = "kstreamsHealthIndicator")
    public KStreamsHealthIndicator kstreamsHealthIndicator(ObjectProvider<StreamsBuilderFactoryBean> provider) {
        final StreamsBuilderFactoryBean factory = provider.getIfAvailable();
        if (factory != null) {
            return new KStreamsHealthIndicator(factory, properties);
        }
        return null;
    }

}
