package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote;

import java.rmi.Remote;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

/**
 * A {@link RemoteQueryableReadOnlyKeyValueStore} that offers local query functionality.
 */
public class LocalQueryableReadOnlyKeyValueStore implements RemoteQueryableReadOnlyKeyValueStore {

    private final StreamsBuilderFactoryBean factory;
    private final HostInfo self;

    LocalQueryableReadOnlyKeyValueStore(final StreamsBuilderFactoryBean factory) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        this.self = Optional.of(factory)
                            .map(StreamsBuilderFactoryBean::getStreamsConfiguration)
                            .map(StreamsConfig::new)
                            .map(config -> config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG))
                            .map(HostInfo::buildFromEndpoint)
                            .orElseThrow();
    }

    /**
     * @return the name to associate with the {@link Remote} reference.
     */
    @Override
    public String reference() {
        return RemoteQueryableReadOnlyKeyValueStore.class.getName();
    }

    /**
     * @return a {@link HostInfo host} that points to itself.
     */
    @Override
    public HostInfo self() {
        return self;
    }

    @Override
    public <K, V> Optional<V> findByKey(final K key, final String storeName) {
        final KafkaStreams streams = factory.getKafkaStreams();
        Objects.requireNonNull(streams, "KafkaStreams [factory.kafkaStreams] must be available.");

        final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> type = keyValueStore();
        final StoreQueryParameters<ReadOnlyKeyValueStore<K, V>> parameters = fromNameAndType(storeName, type);
        final ReadOnlyKeyValueStore<K, V> store = streams.store(parameters);
        final V value = store.get(key);
        return Optional.ofNullable(value);
    }

    @Override
    public QueryableStoreType<?> type() {
        return keyValueStore();
    }
}
