package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.util.Assert;

import static org.apache.kafka.streams.StoreQueryParameters.*;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

public class DefaultQueryableReadyOnlyStore<K, V> implements QueryableReadOnlyKeyValueStore<K, V> {

    private final String storeName;
    private final KafkaStreams streams;

    DefaultQueryableReadyOnlyStore(final String storeName, final KafkaStreams streams) {
        Assert.hasText(storeName, "Attribute [storeName] is required and cannot be empty.");
        this.storeName = storeName;
        this.streams = Objects.requireNonNull(streams, "Attribute [streams] is required.");
    }

    @Override
    public Optional<V> findByKey(final K key) {
        final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> type = keyValueStore();
        final StoreQueryParameters<ReadOnlyKeyValueStore<K, V>> parameters = fromNameAndType(storeName, type);
        final ReadOnlyKeyValueStore<K, V> store = streams.store(parameters);
        final V value = store.get(key);
        return Optional.ofNullable(value);
    }

    @Override
    public Optional<KeyQueryMetadata> queryMetadataForKey(final K key, final Serializer<K> serializer) {
        final KeyQueryMetadata metadata = streams.queryMetadataForKey(storeName, key, serializer);
        return Optional.ofNullable(metadata)
                       .filter(m -> !KeyQueryMetadata.NOT_AVAILABLE.equals(m));
    }

}
