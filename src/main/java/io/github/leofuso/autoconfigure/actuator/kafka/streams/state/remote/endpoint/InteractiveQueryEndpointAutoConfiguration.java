package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint;

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

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.DefaultRemoteQuerySupport;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.LocalKeyValueStore;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteQuerySupport;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;


@AutoConfiguration(after = {EndpointAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnProperty(prefix = "spring.kafka", name = {"streams.properties." + APPLICATION_SERVER_CONFIG})
public class InteractiveQueryEndpointAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LocalKeyValueStore localKeyValueStore(ObjectProvider<StreamsBuilderFactoryBean> provider) {
        final StreamsBuilderFactoryBean factory = provider.getIfAvailable();
        if (factory != null) {
            return new LocalKeyValueStore(factory);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    public RemoteQuerySupport remoteQuerySupport(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider,
                                                 ObjectProvider<RemoteStateStore> storesProvider,
                                                 ObjectProvider<ConversionService> converterProvider) {

        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        final ConversionService converter = converterProvider.getIfAvailable();

        if (factory != null && converter != null) {
            final Set<RemoteStateStore> stores = storesProvider.stream().collect(Collectors.toSet());
            return new DefaultRemoteQuerySupport(factory, stores, converter);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint(endpoint = ReadOnlyStateStoreEndpoint.class)
    public ReadOnlyStateStoreEndpoint readOnlyStateStoreEndpoint(ObjectProvider<RemoteQuerySupport> provider) {
        final RemoteQuerySupport support = provider.getIfAvailable();
        if (support != null) {
            return new ReadOnlyStateStoreEndpoint(support);
        }
        return null;
    }

}
