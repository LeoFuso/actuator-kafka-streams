package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote;

import javax.annotation.Nullable;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
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
 * A {@link RemoteQueryableReadOnlyKeyValueStore} that offers local query functionality and can be accessed through a
 * {@link java.rmi.Remote RMI} interface.
 */
public class LocalQueryableReadOnlyKeyValueStore implements RemoteQueryableReadOnlyKeyValueStore {

    /**
     * Explicit serialVersionUID for interoperability.
     */
    private static final long serialVersionUID = -5235727626367529380L;

    private final StreamsBuilderFactoryBean factory;
    private final HostInfo self;

    @Nullable
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static Registry registry;

    @Nullable
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static Remote remoteObject;

    /**
     * Creates a new instance of this {@link RemoteQueryableReadOnlyKeyValueStore store}. Will throw a
     * {@link NullPointerException} in case of missing required configurations, including the ability to create a
     * {@link HostInfo host}, necessary to expose this {@link RemoteQueryableReadOnlyKeyValueStore store} to the
     * {@link java.rmi.registry.Registry registry}.
     *
     * @param factory used to extract the necessary configurations to a new instance.
     */
    public LocalQueryableReadOnlyKeyValueStore(final StreamsBuilderFactoryBean factory) {
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
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LocalQueryableReadOnlyKeyValueStore)) {
            return false;
        }
        final LocalQueryableReadOnlyKeyValueStore that = (LocalQueryableReadOnlyKeyValueStore) o;
        return reference().equals(that.reference());
    }

    @Override
    public void hold(final Registry registry, final Remote remoteObject) throws RemoteException {
        LocalQueryableReadOnlyKeyValueStore.registry = registry;
        LocalQueryableReadOnlyKeyValueStore.remoteObject = remoteObject;
    }

    @Override
    public String reference() {
        return RemoteQueryableReadOnlyKeyValueStore.class.getName();
    }

    @Override
    public HostInfo self() {
        return self;
    }

    @Override
    public <K, V> V findByKey(final K key, final String storeName) {
        final KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            throw new StreamsNotStartedException("KafkaStreams [factory.kafkaStreams] must be available.");
        }

        final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> type = keyValueStore();
        final StoreQueryParameters<ReadOnlyKeyValueStore<K, V>> parameters = fromNameAndType(storeName, type);
        final ReadOnlyKeyValueStore<K, V> store = streams.store(parameters);
        return store.get(key);
    }
}
