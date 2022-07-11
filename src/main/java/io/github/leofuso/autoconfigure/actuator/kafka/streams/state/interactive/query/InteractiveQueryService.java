package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableStore;

public interface InteractiveQueryService {

    <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName);

    <K, T, R extends RemoteQueryableStore> Optional<R> store(K key, String store, QueryableStoreType<T> type);

    <K, T, S extends Serde<K>, R extends RemoteQueryableStore> Optional<R> store(K key, Class<S> serdeClass, String store, QueryableStoreType<T> type);

    /**
     * @param host used to lookup.
     * @return a {@link QueryableStore QueryableStore} associated with given {@link HostInfo host}. Can be a local one,
     * if the given {@link HostInfo host} points to itself, or a {@link RemoteQueryableStore remote} one, if the given
     * {@link HostInfo host} points to somewhere else.
     */
    <T, S extends RemoteQueryableStore> Optional<S> store(HostInfo host, QueryableStoreType<T> storeType);

}
