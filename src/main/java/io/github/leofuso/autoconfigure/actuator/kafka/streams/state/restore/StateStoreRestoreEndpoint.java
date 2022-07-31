package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import static org.springframework.boot.actuate.endpoint.annotation.Selector.Match.ALL_REMAINING;

/**
 * Actuator endpoint for {@link org.apache.kafka.streams.processor.StateRestoreListener restoration} queries.
 */
@SuppressWarnings("unused")
@Endpoint(id = "statestorerestore")
public class StateStoreRestoreEndpoint {

    private final ObjectProvider<StateStoreRestoreRepository> provider;

    /**
     * Constructs a new StateStoreRestoreEndpoint instance.
     *
     * @param provider to delegate the queries to.
     */
    public StateStoreRestoreEndpoint(final ObjectProvider<StateStoreRestoreRepository> provider) {
        this.provider = Objects.requireNonNull(provider, "ObjectProvider [repository] is required.");
    }

    /**
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states.
     */
    @ReadOperation
    public List<Map<String, Object>> all() {
        final StateStoreRestoreRepository repository = provider.getIfAvailable();
        if (repository == null) {
            return List.of();
        }
        return repository.list();
    }

    /**
     * @param storeName {@link org.apache.kafka.streams.processor.StateStore StateStore} name to query for restoration
     *                  states.
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states, if
     * any.
     */
    @ReadOperation
    public Map<String, Object> findByStoreName(@Selector(match = ALL_REMAINING) String storeName) {
        final StateStoreRestoreRepository repository = provider.getIfAvailable();
        if (repository == null) {
            return Map.of();
        }
        return repository.findByStoreName(storeName);
    }
}
