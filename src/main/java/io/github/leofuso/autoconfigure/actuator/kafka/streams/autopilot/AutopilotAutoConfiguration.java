package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
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

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotAutoConfiguration.INDICATOR;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;

@AutoConfiguration(after = {KafkaStreamsDefaultConfiguration.class}, before = {HealthEndpointAutoConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnEnabledHealthIndicator(INDICATOR)
@EnableConfigurationProperties(AutopilotConfigurationProperties.class)
public class AutopilotAutoConfiguration {

    public static final String INDICATOR = "autopilot";

    private final AutopilotConfigurationProperties properties;

    public AutopilotAutoConfiguration(final AutopilotConfigurationProperties properties) {
        this.properties = properties;
    }

    @Bean(initMethod = "initialize", destroyMethod = "shutdown")
    @DependsOn({DEFAULT_STREAMS_BUILDER_BEAN_NAME})
    @ConditionalOnMissingBean(Autopilot.class)
    public Autopilot autopilot(ObjectProvider<StreamsBuilderFactoryBean> provider) {
        final StreamsBuilderFactoryBean factory = provider.getIfAvailable();
        if (factory != null) {
            return new DefaultAutopilot(factory, properties);
        }
        return null;
    }

    @Bean
    @DependsOn({DEFAULT_STREAMS_BUILDER_BEAN_NAME})
    @ConditionalOnAvailableEndpoint
    public AutopilotThreadEndpoint autopilotthreadEndpoint(ObjectProvider<Autopilot> provider) {
        final Autopilot autopilot = provider.getIfAvailable();
        if (autopilot != null) {
            return new AutopilotThreadEndpoint(autopilot);
        }
        return null;
    }

    @Bean
    @DependsOn({DEFAULT_STREAMS_BUILDER_BEAN_NAME})
    @ConditionalOnMissingBean(name = "autopilotHealthIndicator")
    public AutopilotHealthIndicator autopilotHealthIndicator(ObjectProvider<Autopilot> provider) {
        final Autopilot autopilot = provider.getIfAvailable();
        if (autopilot != null) {
            return new AutopilotHealthIndicator(autopilot);
        }
        return null;
    }
}
