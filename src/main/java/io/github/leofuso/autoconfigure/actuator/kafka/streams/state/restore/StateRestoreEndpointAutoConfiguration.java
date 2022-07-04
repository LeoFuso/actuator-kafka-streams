package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.time.Clock;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.streams.processor.StateRestoreListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@AutoConfiguration(after = {EndpointAutoConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnAvailableEndpoint(endpoint = StateStoreRestoreEndpoint.class)
public class StateRestoreEndpointAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentStateStoreRestoreCarrier stateRestoreRepository(ObjectProvider<Clock> clockProvider) {
        final Clock clock = clockProvider.getIfAvailable(Clock::systemUTC);
        return new ConcurrentStateStoreRestoreCarrier(clock);
    }

    @Bean
    @ConditionalOnMissingBean(CompositeStateRestoreListener.class)
    public CompositeStateRestoreListener compositeStateRestoreListener(ObjectProvider<StateRestoreListener> restoreListeners) {
        final Set<StateRestoreListener> orderedListeners = restoreListeners.orderedStream().collect(Collectors.toSet());
        return new DefaultCompositeStateRestoreListener(orderedListeners);
    }

    @Bean
    @ConditionalOnMissingBean(name = "compositeStateRestoreListenerConfigurer")
    public StreamsBuilderFactoryBeanConfigurer compositeStateRestoreListenerConfigurer(ObjectProvider<CompositeStateRestoreListener> compositeProvider) {
        final CompositeStateRestoreListener listener = compositeProvider.getIfAvailable();
        if (listener != null) {
            return fb -> fb.setStateRestoreListener(listener);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean(name = "stateStoreRestoreEndpoint")
    public StateStoreRestoreEndpoint stateStoreRestoreEndpoint(ObjectProvider<StateStoreRestoreRepository> repositoryProvider) {
        final StateStoreRestoreRepository repository = repositoryProvider.getIfAvailable();
        if (repository != null) {
            return new StateStoreRestoreEndpoint(repository);
        }
        return null;
    }

}
