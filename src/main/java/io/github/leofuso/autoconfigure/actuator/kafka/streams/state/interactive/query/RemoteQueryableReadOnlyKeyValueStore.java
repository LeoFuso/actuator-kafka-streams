package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Optional;

import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.BeanFactory;


public interface RemoteQueryableReadOnlyKeyValueStore
        extends RemoteQueryableStore<RemoteQueryableReadOnlyKeyValueStore> {

    static RemoteQueryableReadOnlyKeyValueStore local(BeanFactory factory) {
        return new LocalQueryableReadOnlyKeyValueStore(factory);
    }

    static RemoteQueryableReadOnlyKeyValueStore remote(BeanFactory factory, HostInfo info) {
        return new HttpQueryableReadOnlyKeyValueStore(factory, info);
    }

    <K, V> Optional<V> findByKey(K key, String storeName);

    <K, V> Optional<KeyQueryMetadata> queryMetadataForKey(K key, String storeName);

}
