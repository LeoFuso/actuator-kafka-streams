package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.util.Optional;

import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.CompositeStateAutoConfiguration;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for {@link Autopilot}.
 */
@AutoConfiguration(before = {CompositeStateAutoConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@EnableConfigurationProperties(AutopilotConfiguration.class)
public class AutopilotAutoConfiguration {

    private final KafkaStreamsConfiguration streamsConfig;
    private final AutopilotConfiguration autopilotConfig;

    /**
     * Constructs an {@link EnableAutoConfiguration Auto-configuration} instance.
     *
     * @param streamsConfig   used by {@link Autopilot}.
     * @param autopilotConfig used by {@link Autopilot}.
     */
    public AutopilotAutoConfiguration(KafkaStreamsConfiguration streamsConfig, AutopilotConfiguration autopilotConfig) {
        this.streamsConfig = streamsConfig;
        this.autopilotConfig = autopilotConfig;
    }

    /**
     * @return a {@link AutopilotSupport} capable of providing access to an {@link Autopilot} automated instance.
     */
    @Bean
    @ConditionalOnMissingBean(AutopilotSupport.class)
    public AutopilotSupport autopilotSupport() {
        return AutopilotSupport.automated(streamsConfig, autopilotConfig);
    }

    @Bean
    @ConditionalOnMissingBean
    public StateListener autopilotSupportAutomationHook(ObjectProvider<AutopilotSupport> provider) {
        final AutopilotSupport support = provider.getIfAvailable();
        if (support != null) {
            final Optional<StateListener> listener = support.automationHook();
            return listener.orElse(null);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamsBuilderFactoryBeanCustomizer autopilotSupportLifecycleHook(ObjectProvider<AutopilotSupport> provider) {
        final AutopilotSupport support = provider.getIfAvailable();
        if (support != null) {
            return support.lifecycleHook();
        }
        return null;
    }

    @Bean
    @ConditionalOnAvailableEndpoint
    @ConditionalOnMissingBean(AutopilotThreadEndpoint.class)
    public AutopilotThreadEndpoint autopilotthreadEndpoint(ObjectProvider<AutopilotSupport> provider) {
        final AutopilotSupport support = provider.getIfAvailable();
        if (support != null) {
            return new AutopilotThreadEndpoint(support);
        }
        return null;
    }
}
