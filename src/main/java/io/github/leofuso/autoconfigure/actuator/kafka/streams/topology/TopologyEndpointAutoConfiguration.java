package io.github.leofuso.autoconfigure.actuator.kafka.streams.topology;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@AutoConfigureAfter({EndpointAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class})
public class TopologyEndpointAutoConfiguration {

    @Bean
    @ConditionalOnAvailableEndpoint
    public TopologyEndpoint topologyEndpoint(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider) {
        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        if (factory != null) {
            return new TopologyEndpoint(factory);
        }
        return null;
    }
}
