package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Provides an easy Api to execute queries locally and remotely, without worrying about the lifecycle of all the
 * required components.
 * <p>
 * Due to the nature of this Api, all invocations are relatively expensive and should be performed with care.
 */
public interface RemoteQuerySupport {

    /**
     * @param key        needed in hash function used to pinpoint the correct {@link HostInfo host}.
     * @param serializer needed in hash function used to pinpoint the correct {@link HostInfo host}.
     * @param storeName  to check overall availability.
     * @param <K>        the key type.
     * @return a {@link HostInfo host} that carries a reference of given key paired with given serializer.
     */
    <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName);

    /**
     * Find a specific {@link RemoteStateStore store} by its {@link RemoteStateStore#reference() reference}.
     *
     * @param reference the reference.
     * @param <R>       the {@link RemoteStateStore storeType} type.
     * @return a {@link RemoteStateStore} associated with given {@link RemoteStateStore#reference() reference}.
     */
    <R extends RemoteStateStore> Optional<R> findStore(String reference);

    /**
     * @param host      used for lookups.
     * @param storeType used to compatibility check against the {@link RemoteStateStore store} candidates.
     * @param <R>       the {@link RemoteStateStore storeType} type.
     * @return a {@link RemoteStateStore} associated with given {@link HostInfo host}. Can be a
     * {@link RemoteStateStore local}, if the given {@link HostInfo host} points to itself, or a
     * {@link RemoteStateStore remote} one, if the given {@link HostInfo host} points to somewhere else.
     */
    <R extends RemoteStateStore> Optional<R> findStore(HostInfo host, QueryableStoreType<?> storeType);

    /**
     * Will locate and perform an invocation on the specified {@link RemoteStateStore store}.
     *
     * @param arguments needed to locate a {@link RemoteStateStore store}, and perform the invocation on found
     *                  {@link RemoteStateStore store}.
     * @param <K>       the key type.
     * @param <V>       the value type returned by the performed invocation.
     * @param <R>       the {@link RemoteStateStore store} type.
     * @return a result after performing a successful invocation, or nothing.
     */
    <K, V, R extends RemoteStateStore> CompletableFuture<V> invoke(Arguments<K, V, R> arguments);
}
