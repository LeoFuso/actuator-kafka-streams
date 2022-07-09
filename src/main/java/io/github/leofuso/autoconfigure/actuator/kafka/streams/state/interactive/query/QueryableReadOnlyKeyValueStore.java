package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;

public interface QueryableReadOnlyKeyValueStore<K, V> {

    static <K, V> QueryableReadOnlyKeyValueStore<K, V> readOnlyKeyValueStore(String name, KafkaStreams streams) {
        return new DefaultQueryableReadyOnlyStore<>(name, streams);
    }

    Optional<V> findByKey(K key);

    Optional<KeyQueryMetadata> queryMetadataForKey(K key, Serializer<K> serializer);
}
