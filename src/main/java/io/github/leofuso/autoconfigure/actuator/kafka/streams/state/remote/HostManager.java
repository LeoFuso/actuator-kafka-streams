package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Responsible for managing the lifecycle of all created channels between a {@link HostInfo host} and its
 * {@link RemoteStateStoreStub stub} counterpart.
 */
public interface HostManager {

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

    void shutdown() throws InterruptedException;

}
