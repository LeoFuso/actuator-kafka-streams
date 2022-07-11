package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote;

import java.rmi.Remote;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
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

    /**
     * Creates a new instance of this {@link RemoteQueryableReadOnlyKeyValueStore store}. Will throw a
     * {@link NullPointerException} in case of missing required configurations, including the ability to create a
     * {@link HostInfo host}, necessary to expose this {@link RemoteQueryableReadOnlyKeyValueStore store} to the
     * {@link java.rmi.registry.Registry registry}.
     *
     * @param factory used to extract the necessary configurations to a new instance.
     */
    LocalQueryableReadOnlyKeyValueStore(final StreamsBuilderFactoryBean factory) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        this.self =
                Optional.of(factory)
                        .map(StreamsBuilderFactoryBean::getStreamsConfiguration)
                        .map(StreamsConfig::new)
                        .map(config -> config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG))
                        .map(HostInfo::buildFromEndpoint)
                        .orElseThrow();
    }

    @Override
    public QueryableStoreType<?> type() {
        return keyValueStore();
    }

    @Override
    public int hashCode() {
        return Objects.hash(reference());
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof final LocalQueryableReadOnlyKeyValueStore that)) {
            return false;
        }
        return reference().equals(that.reference());
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
        if (streams == null) {
            throw new StreamsNotStartedException("KafkaStreams [factory.kafkaStreams] must be available.");
        }

        final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> type = keyValueStore();
        final StoreQueryParameters<ReadOnlyKeyValueStore<K, V>> parameters = fromNameAndType(storeName, type);
        final ReadOnlyKeyValueStore<K, V> store = streams.store(parameters);
        final V value = store.get(key);
        return Optional.ofNullable(value);
    }
}
