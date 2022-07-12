package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.rmi.RemoteException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.core.convert.ConversionService;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.util.Assert;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.AdditionalSerdesConfig;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableStore;

/**
 * Standard implementation of the {@link InteractiveQuery} Api.
 */
public class InteractiveQueryImpl implements InteractiveQuery {

    private final StreamsBuilderFactoryBean factory;
    private final ConversionService converter;
    private final Set<RemoteQueryableStore> remoteStores;

    private final StreamsConfig streamsConfig;
    private final AdditionalSerdesConfig additionalSerdesConfig;

    public InteractiveQueryImpl(StreamsBuilderFactoryBean factory, Set<RemoteQueryableStore> stores, ConversionService converter) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        final Properties properties = factory.getStreamsConfiguration();
        Assert.state(properties != null, "Streams configuration properties must not be null.");

        this.converter = Objects.requireNonNull(converter, "ConversionService [converter] is required");
        this.remoteStores = Optional.ofNullable(stores)
                                    .orElseGet(Set::of);

        this.streamsConfig = new StreamsConfig(properties);
        this.additionalSerdesConfig = new AdditionalSerdesConfig(properties);
    }

    @Override
    public <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName) {
        return Optional.ofNullable(factory.getKafkaStreams())
                       .map(streams -> streams.queryMetadataForKey(storeName, key, serializer))
                       .filter(m -> !KeyQueryMetadata.NOT_AVAILABLE.equals(m))
                       .map(KeyQueryMetadata::activeHost);
    }

    @Override
    public <R extends RemoteQueryableStore> Optional<R> findCompatibleStore(HostInfo host, QueryableStoreType<?> type) {
        try {
            for (RemoteQueryableStore store : remoteStores) {
                final boolean isCompatible = store.isCompatible(type);
                if (isCompatible) {
                    final R stub = store.stub(host);
                    return Optional.of(stub);
                }
            }
        } catch (RemoteException ex) {
            throw new RuntimeException(ex);
        }
        return Optional.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V, R extends RemoteQueryableStore> Optional<V> execute(Action<K, V, R> action) {

        final Class<Serde<K>> nonDefaultKeySerdeClass = action.getKeySerdeClass();

        final Serde<K> serde;
        if (nonDefaultKeySerdeClass != null) {
            serde = additionalSerdesConfig.serde(nonDefaultKeySerdeClass);
        } else {
            serde = streamsConfig.defaultKeySerde();
        }

        final String stringifiedKey = action.getStringifiedKey();
        final Class<K> serdeUnderlyingSignatureType = AdditionalSerdesConfig.serdeType(serde);
        final K key = converter.convert(stringifiedKey, serdeUnderlyingSignatureType);

        final Serializer<K> serializer = serde.serializer();
        final String storeName = action.getStoreName();
        final QueryableStoreType<?> storeType = action.getQueryableStoreType();
        return findHost(key, serializer, storeName)
                .flatMap(host -> findCompatibleStore(host, storeType))
                .map(untypedRemoteStore -> (R) untypedRemoteStore)
                .flatMap(remoteStore -> action.getQuery().apply(key, remoteStore));
    }

}
