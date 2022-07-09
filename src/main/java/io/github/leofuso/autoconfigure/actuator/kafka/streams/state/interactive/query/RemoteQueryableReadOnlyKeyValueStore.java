package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.QueryableReadOnlyKeyValueStore.readOnlyKeyValueStore;


public interface RemoteQueryableReadOnlyKeyValueStore
        extends RemoteQueryableStore<RemoteQueryableReadOnlyKeyValueStore> {

    static RemoteQueryableReadOnlyKeyValueStore instantiate(final StreamsConfig config, final KafkaStreams streams) {
        return new DefaultRemoteQueryableReadOnlyKeyValueStore(config, streams);
    }

    <K, V> Optional<V> findByKey(K key, String storeName);

    <K, V> Optional<KeyQueryMetadata> queryMetadataForKey(K key, String storeName);


    class DefaultRemoteQueryableReadOnlyKeyValueStore implements RemoteQueryableReadOnlyKeyValueStore {

        private static final long serialVersionUID = -4787108556148621714L;

        private final StreamsConfig config;
        private final HostInfo info;
        private final KafkaStreams streams;

        DefaultRemoteQueryableReadOnlyKeyValueStore(StreamsConfig config, KafkaStreams streams) {
            this.config = Objects.requireNonNull(config, "Attribute [config] is required.");
            this.streams = Objects.requireNonNull(streams, "Attribute [streams] is required.");

            final String serverConfig = config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG);
            this.info = HostInfo.buildFromEndpoint(serverConfig);
        }

        @Override
        public HostInfo info() {
            return this.info;
        }

        @Override
        public String name() {
            return RemoteQueryableReadOnlyKeyValueStore.class.getName();
        }

        @Override
        public <K, V> Optional<V> findByKey(final K key, final String storeName) {
            final QueryableReadOnlyKeyValueStore<K, V> store = readOnlyKeyValueStore(storeName, streams);
            return store.findByKey(key);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> Optional<KeyQueryMetadata> queryMetadataForKey(final K key, final String storeName) {
            final QueryableReadOnlyKeyValueStore<K, V> store = readOnlyKeyValueStore(storeName, streams);
            try (final Serde<K> keySerde = (Serde<K>) config.defaultKeySerde()) {
                return store.queryMetadataForKey(key, keySerde.serializer());
            }
        }
    }
}
