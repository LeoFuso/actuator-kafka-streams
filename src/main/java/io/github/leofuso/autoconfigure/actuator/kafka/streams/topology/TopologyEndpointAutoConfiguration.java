package io.github.leofuso.autoconfigure.actuator.kafka.streams.topology;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for {@link TopologyEndpoint Topology endpoint}.
 */
@AutoConfiguration(after = {EndpointAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
public class TopologyEndpointAutoConfiguration {

    /**
     * Main bean factory for the {@link TopologyEndpoint}.
     * @param factoryProvider used to create a {@link TopologyEndpoint}.
     * @return a new {@link TopologyEndpoint}.
     */
    @Bean
    @DependsOn({DEFAULT_STREAMS_BUILDER_BEAN_NAME})
    @ConditionalOnAvailableEndpoint
    public TopologyEndpoint topologyEndpoint(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider) {
        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        if (factory != null) {
            return new TopologyEndpoint(factory);
        }
        return null;
    }
}
