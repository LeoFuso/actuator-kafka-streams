package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.util.List;
import java.util.Map;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import static org.springframework.boot.actuate.endpoint.annotation.Selector.Match.ALL_REMAINING;

/**
 * Actuator endpoint for {@link org.apache.kafka.streams.processor.StateRestoreListener restoration} queries.
 */
@Endpoint(id = "statestorerestore")
public class StateStoreRestoreEndpoint {

    private final StateStoreRestoreRepository repository;

    public StateStoreRestoreEndpoint(final StateStoreRestoreRepository repository) {
        this.repository = repository;
    }

    /**
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states.
     */
    @ReadOperation
    public List<Map<String, Object>> all() {
        return repository.list();
    }

    /**
     * @param storeName {@link org.apache.kafka.streams.processor.StateStore StateStore} name to query for restoration states.
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states, if any.
     */
    @ReadOperation
    public Map<String, Object> findByStoreName(@Selector(match = ALL_REMAINING) String storeName) {
        return repository.findByStoreName(storeName);
    }
}
