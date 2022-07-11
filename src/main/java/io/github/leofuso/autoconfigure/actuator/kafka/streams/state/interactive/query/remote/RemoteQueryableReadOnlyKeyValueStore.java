package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote;

import java.util.Optional;

import org.apache.kafka.streams.state.HostInfo;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.QueryableStore;

public interface RemoteQueryableReadOnlyKeyValueStore extends RemoteQueryableStore {

    @Override
    default String reference() {
        return RemoteQueryableReadOnlyKeyValueStore.class.getName();
    }

    @Override
    HostInfo self();

    <K, V> Optional<V> findByKey(K key, String storeName);

}
