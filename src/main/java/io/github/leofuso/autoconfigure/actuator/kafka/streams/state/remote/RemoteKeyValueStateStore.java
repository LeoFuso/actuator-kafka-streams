package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.timestampedKeyValueStore;

/**
 * A simplified and remote implementation of
 * {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValueStore} and
 * {@link org.apache.kafka.streams.state.TimestampedKeyValueStore TimestampedKeyValueStore}.
 */
public interface RemoteKeyValueStateStore extends RemoteStateStore {

    /**
     * Get the value corresponding to this key.
     *
     * @param key       The key to fetch
     * @param storeName the store to query on.
     * @param <K>       the key type.
     * @param <V>       the value type.
     * @return The value or null if no value is found.
     *
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    <K, V> CompletableFuture<V> findOne(K key, String storeName);

    /**
     * Get the (value/timestamp) corresponding to this key.
     *
     * @param key       The key to fetch
     * @param storeName the store to query on.
     * @param <K>       the key type.
     * @param <V>       the value type.
     * @return The (value/timestamp) or null if no value is found.
     *
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    <K, V> CompletableFuture<ValueAndTimestamp<V>> findOneTimestamped(K key, String storeName);

    default String reference() {
        return RemoteKeyValueStateStore.class.getName();
    }

    /**
     * @return a {@link Set} containing all {@link QueryableStoreType} associated with this
     * {@link RemoteKeyValueStateStore}.
     */
    default Set<Class<? extends QueryableStoreType<?>>> types() {
        @SuppressWarnings("unchecked")
        final Set<Class<? extends QueryableStoreType<?>>> types = Set.of(
                (Class<? extends QueryableStoreType<?>>) keyValueStore().getClass(),
                (Class<? extends QueryableStoreType<?>>) timestampedKeyValueStore().getClass()
        );
        return types;
    }

    /**
     * @param host used to locate the {@link RemoteKeyValueStateStore store}.
     * @param <R>  the type of the wanted {@link RemoteKeyValueStateStore store}.
     * @return a stub for this {@link RemoteKeyValueStateStore store}, or the real one, if a proxy creation is
     * unnecessary.
     */
    default <R extends RemoteStateStore> R stub(HostInfo host) {
        final boolean unnecessaryStub = self().equals(host);
        if (unnecessaryStub) {
            @SuppressWarnings("unchecked")
            final R self = (R) this;
            return self;
        }

        final String hostname = host.host();
        final int port = host.port();

        /* Maybe we should store this in a Cache to maintain the existent connections? */
        final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(hostname, port)
                .usePlaintext()
                .build();

        final StateStoreGrpc.StateStoreStub stub = StateStoreGrpc.newStub(channel);
        @SuppressWarnings("unchecked")
        final R remoteStub = (R) new KeyValueStateStoreStub(stub, host);
        return remoteStub;
    }

    default Integer getMethodKey() {
        return 2;
    }

}
