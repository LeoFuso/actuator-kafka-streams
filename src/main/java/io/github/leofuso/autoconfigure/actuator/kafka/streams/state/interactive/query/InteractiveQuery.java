package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableStore;

/**
 * Provides an easy Api to execute queries remotely, without worrying about the lifecycle of all the required
 * components.
 * <p>
 * Due to the nature of the query Api this is a relative expensive operation and should be invoked with care.
 */
public interface InteractiveQuery {

    /**
     * @param key        needed in hash function used to pinpoint the correct {@link HostInfo host}.
     * @param serializer needed in hash function used to pinpoint the correct {@link HostInfo host}.
     * @param storeName  to check overall availability.
     * @param <K>        the key type.
     * @return a {@link HostInfo host} that carries a reference of given key paired with given serializer.
     */
    <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName);

    /**
     * @param host      used for lookup operations, if necessary.
     * @param storeType used to compatibility check against the {@link RemoteQueryableStore store} candidates.
     * @param <R>       the {@link QueryableStore} type.
     * @return a {@link QueryableStore} associated with given {@link HostInfo host}. Can be a
     * {@link RemoteQueryableStore local}, if the given {@link HostInfo host} points to itself, or a
     * {@link RemoteQueryableStore remote} one, if the given {@link HostInfo host} points to somewhere else.
     */
    <R extends RemoteQueryableStore> Optional<R> findCompatibleStore(HostInfo host, QueryableStoreType<?> storeType);

    /**
     * Will locate and execute a {@link Action#getQuery() query} on the specified {@link RemoteQueryableStore store}.
     *
     * @param action used as carrier of all needed specifications to locate a {@link RemoteQueryableStore store}, and
     *               the {@link Action#getQuery() query} itself.
     * @param <K>    the key type.
     * @param <V>    the value type returned by the performed {@link Action#getQuery() query}.
     * @param <R>    the {@link RemoteQueryableStore store} type.
     * @return a result after performing the {@link Action} successfully, or nothing.
     */
    <K, V, R extends RemoteQueryableStore> Optional<V> execute(Action<K, V, R> action);

}
