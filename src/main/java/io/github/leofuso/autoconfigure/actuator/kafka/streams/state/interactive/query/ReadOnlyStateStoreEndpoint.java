package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import javax.annotation.Nullable;

import java.rmi.RemoteException;
import java.util.Map;

import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableReadOnlyKeyValueStore;

import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

/**
 * Actuator endpoint for querying {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValue} stores.
 */
@Endpoint(id = "readonlystatestore")
public class ReadOnlyStateStoreEndpoint {

    private static final String ERROR_MESSAGE_KEY = "message";

    private final InteractiveQuery executor;

    public ReadOnlyStateStoreEndpoint(final InteractiveQuery executor) {
        this.executor = executor;
    }

    /**
     * Query for a value associated with given key and store.
     *
     * @param store of the {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore queryable store}.
     * @param key   to query for.
     * @param serde the key class. Restricted to supported {@link org.apache.kafka.common.serialization.Serdes serdes}
     *              types.
     * @return the value associated with the key, if any. Will encapsulate eventual
     * {@link Exception#getMessage() exception's messages} into a response object.
     */
    @ReadOperation
    public <K, V> Map<String, String> find(@Selector String store, @Selector String key, @Nullable String serde) {
        try {

            var action =
                    Action.<K, V, RemoteQueryableReadOnlyKeyValueStore>performOn(store, keyValueStore())
                               .usingStringifiedKey(key)
                               .withKeySerdeClass(serde)
                               .aQuery((k, s) -> {
                                   try {
                                       return s.findByKey(k, store);
                                   } catch (RemoteException e) {
                                       throw new RuntimeException(e);
                                   }
                               });

            return executor.execute(action)
                           .map(value -> Map.of(key, value.toString()))
                           .orElseGet(() -> Map.of(key, Strings.EMPTY));

        } catch (ClassNotFoundException ex) {
            return Map.of(ERROR_MESSAGE_KEY, "ClassNotFoundException: " + ex.getMessage());
        } catch (Exception ex) {
            return Map.of(ERROR_MESSAGE_KEY, ex.getMessage());
        }
    }

}
