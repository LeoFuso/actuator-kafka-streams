package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.util.Assert;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.QueryableReadOnlyKeyValueStore.readOnlyKeyValueStore;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;

/**
 * A {@link RemoteQueryableReadOnlyKeyValueStore} that offers local query functionality.
 */
class LocalQueryableReadOnlyKeyValueStore implements RemoteQueryableReadOnlyKeyValueStore {

    private final BeanFactory beanFactory;

    private final StreamsConfig config;
    private final KafkaStreams streams;

    /**
     * Local {@link HostInfo host information}.
     */
    private final HostInfo info;

    LocalQueryableReadOnlyKeyValueStore(final BeanFactory beanFactory) {
        this.beanFactory = Objects.requireNonNull(beanFactory, "BeanFactory [beanFactory] is required.");

        final StreamsBuilderFactoryBean factory = beanFactory
                .getBean(DEFAULT_STREAMS_BUILDER_BEAN_NAME, StreamsBuilderFactoryBean.class);

        Assert.isTrue(
                factory.isRunning(),
                "StreamsBuilderFactoryBean [" + DEFAULT_STREAMS_BUILDER_BEAN_NAME + "] must be running."
        );

        final Properties properties = factory.getStreamsConfiguration();
        Objects.requireNonNull(properties, "Properties [factory.properties] must not be null.");

        final StreamsConfig config = new StreamsConfig(properties);
        final KafkaStreams streams = factory.getKafkaStreams();

        this.config = config;
        this.streams = Objects.requireNonNull(streams, "KafkaStreams [factory.kafkaStreams] must not be null.");

        final String serverConfig = config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG);
        this.info = HostInfo.buildFromEndpoint(serverConfig);
        Objects.requireNonNull(info, "HostInfo [info] is required.");
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

    @Override
    public BeanFactory beanFactory() {
        return beanFactory;
    }
}