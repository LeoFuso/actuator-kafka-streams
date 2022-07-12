package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Set;
import java.util.stream.Collectors;

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
import org.springframework.core.convert.ConversionService;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.LocalQueryableReadOnlyKeyValueStore;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableReadOnlyKeyValueStore;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableStore;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;


@AutoConfiguration(after = {EndpointAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnProperty(prefix = "spring.kafka", name = {"streams.properties." + APPLICATION_SERVER_CONFIG})
public class InteractiveQueryEndpointAutoConfiguration {

    @ConditionalOnMissingBean
    @Bean(name = "remoteQueryableReadOnlyKeyValueStore", initMethod = "initialize", destroyMethod = "destroy")
    public RemoteQueryableReadOnlyKeyValueStore keyValueStore(ObjectProvider<StreamsBuilderFactoryBean> provider) {
        final StreamsBuilderFactoryBean factory = provider.getIfAvailable();
        if (factory != null) {
            return new LocalQueryableReadOnlyKeyValueStore(factory);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    public InteractiveQuery interactiveQuery(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider,
                                             ObjectProvider<RemoteQueryableStore> storesProvider,
                                             ObjectProvider<ConversionService> converterProvider) {

        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        final ConversionService converter = converterProvider.getIfAvailable();

        if (factory != null && converter != null) {
            final Set<RemoteQueryableStore> stores = storesProvider.stream().collect(Collectors.toSet());
            return new InteractiveQueryImpl(factory, stores, converter);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint(endpoint = ReadOnlyStateStoreEndpoint.class)
    public ReadOnlyStateStoreEndpoint readOnlyStateStoreEndpoint(ObjectProvider<InteractiveQuery> provider) {
        final InteractiveQuery interactiveQuery = provider.getIfAvailable();
        if (interactiveQuery != null) {
            return new ReadOnlyStateStoreEndpoint(interactiveQuery);
        }
        return null;
    }

}
