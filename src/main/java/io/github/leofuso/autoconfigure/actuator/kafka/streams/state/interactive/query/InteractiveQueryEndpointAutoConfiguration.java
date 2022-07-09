package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;


@AutoConfiguration(after = {EndpointAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
public class InteractiveQueryEndpointAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint(endpoint = StateStoreReadOnlyEndpoint.class)
    @ConditionalOnProperty(prefix = "spring.kafka.stream.properties." + StreamsConfig.APPLICATION_SERVER_CONFIG)
    public <V> StateStoreReadOnlyEndpoint stateStoreReadOnlyEndpoint(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider) {
        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        if (factory != null) {
            return new StateStoreReadOnlyEndpoint(factory);
        }
        return null;
    }

}
