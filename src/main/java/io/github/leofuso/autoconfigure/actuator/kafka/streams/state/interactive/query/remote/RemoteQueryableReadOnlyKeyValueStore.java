package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote;

import java.rmi.RemoteException;
import java.util.Optional;

import org.apache.kafka.streams.errors.InvalidStateStoreException;

/**
 * A simplified and {@link RemoteQueryableStore remote} implementation of a
 * {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValueStore}.
 */
public interface RemoteQueryableReadOnlyKeyValueStore extends RemoteQueryableStore {

    @Override
    default String reference() throws RemoteException {
        return RemoteQueryableReadOnlyKeyValueStore.class.getName();
    }

    /**
     * Get the value corresponding to this key.
     *
     * @param key       The key to fetch
     * @param storeName the store to query to.
     * @param <K>       the key type.
     * @param <V>       the value type.
     * @return The value or null if no value is found.
     *
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    <K, V> Optional<V> findByKey(K key, String storeName) throws RemoteException;

}
