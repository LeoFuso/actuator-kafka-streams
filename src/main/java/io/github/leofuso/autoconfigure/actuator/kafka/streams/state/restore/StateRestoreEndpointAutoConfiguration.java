package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.time.Clock;

import org.apache.kafka.streams.processor.StateRestoreListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.CompositeStateAutoConfiguration;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for {@link StateRestoreListener} feature and its endpoint.
 */
@AutoConfiguration(
        before = {CompositeStateAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class},
        after = {EndpointAutoConfiguration.class}
)
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class, Endpoint.class})
@ConditionalOnAvailableEndpoint(endpoint = StateStoreRestoreEndpoint.class)
public class StateRestoreEndpointAutoConfiguration {

    /**
     * Main bean factory for the {@link StateRestoreListener}.
     *
     * @param provider used to create a {@link StateRestoreListener}.
     * @return a new {@link StateRestoreListener}.
     */
    @Bean
    @ConditionalOnMissingBean(StateStoreRestoreRepository.class)
    public StateRestoreListener stateRestoreRepository(ObjectProvider<Clock> provider) {
        final Clock clock = provider.getIfAvailable(Clock::systemUTC);
        return new ConcurrentStateStoreRestoreCarrier(clock);
    }

    /**
     * Main bean factory for the {@link StateStoreRestoreEndpoint}.
     *
     * @param provider used to create a {@link StateStoreRestoreEndpoint}.
     * @return a new {@link StateStoreRestoreEndpoint}.
     */
    @Bean
    @ConditionalOnMissingBean(StateStoreRestoreEndpoint.class)
    public StateStoreRestoreEndpoint stateStoreRestoreEndpoint(ObjectProvider<StateStoreRestoreRepository> provider) {
        final StateStoreRestoreRepository repository = provider.getIfAvailable();
        if (repository != null) {
            return new StateStoreRestoreEndpoint(repository);
        }
        return null;
    }

}

