package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.core.convert.ConversionService;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.util.Assert;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.AdditionalSerdesConfig;

/**
 * Default implementation of {@link RemoteQuerySupport Api}.
 */
public class DefaultRemoteQuerySupport implements RemoteQuerySupport {

    private final StreamsBuilderFactoryBean factory;
    private final Set<RemoteStateStore> stores;

    private final ConversionService converter;

    private final StreamsConfig streamsConfig;
    private final AdditionalSerdesConfig additionalSerdesConfig;

    public DefaultRemoteQuerySupport(StreamsBuilderFactoryBean factory, Set<RemoteStateStore> stores, ConversionService converter) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        this.converter = Objects.requireNonNull(converter, "ConversionService [converter] is required");

        final Properties properties = factory.getStreamsConfiguration();
        Assert.state(properties != null, "Streams configuration properties must not be null.");

        this.stores = Optional.ofNullable(stores).orElseGet(Set::of);
        this.streamsConfig = new StreamsConfig(properties);
        this.additionalSerdesConfig = new AdditionalSerdesConfig(properties);
    }

    @Override
    public <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName) {
        final KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            throw new StreamsNotStartedException("KafkaStreams [factory.kafkaStreams] must be available.");
        }

        final KeyQueryMetadata metadata = streams.queryMetadataForKey(storeName, key, serializer);
        final boolean notAvailable = metadata.equals(KeyQueryMetadata.NOT_AVAILABLE);
        if (notAvailable) {
            return Optional.empty();
        }

        final HostInfo host = metadata.activeHost();
        return Optional.of(host);
    }

    @Override
    public <R extends RemoteStateStore> Optional<R> findStore(final String reference) {
        for (RemoteStateStore store : stores) {
            final String storeReference = store.reference();
            final boolean isCompatible = storeReference.equals(reference);
            if (isCompatible) {
                @SuppressWarnings("unchecked")
                final R localStore = (R) store;
                return Optional.of(localStore);
            }
        }
        return Optional.empty();
    }

    @Override
    public <R extends RemoteStateStore> Optional<R> findStore(HostInfo host, QueryableStoreType<?> storeType) {
        for (RemoteStateStore store : stores) {
            final boolean isCompatible = store.isCompatible(storeType);
            if (isCompatible) {
                final R stub = store.stub(host);
                return Optional.of(stub);
            }
        }
        return Optional.empty();
    }

    @Override
    public <K, V, R extends RemoteStateStore> CompletableFuture<V> invoke(final Arguments<K, V, R> arguments) {
        try (final Serde<K> serde = resolveKeySerde(arguments)) {

            final String stringifiedKey = arguments.getStringifiedKey();
            final Class<K> serdeUnderlyingSignatureType = AdditionalSerdesConfig.underlyingSerdeType(serde);
            final K key = converter.convert(stringifiedKey, serdeUnderlyingSignatureType);

            final Serializer<K> serializer = serde.serializer();
            final String storeName = arguments.getStoreName();
            final QueryableStoreType<?> storeType = arguments.getQueryableStoreType();

            return findHost(key, serializer, storeName)
                    .flatMap(host -> findStore(host, storeType))
                    .map(untypedStore -> {
                        @SuppressWarnings("unchecked")
                        final R store = (R) untypedStore;
                        return store;
                    })
                    .map(store -> {
                             final BiFunction<K, R, CompletableFuture<V>> invocation = arguments.getInvocation();
                             return invocation.apply(key, store);
                         }
                    )
                    .orElseGet(() -> {
                        /* Define a more suitable exception */
                        final IllegalStateException exception = new IllegalStateException();
                        return CompletableFuture.failedFuture(exception);
                    });
        }
    }

    private <K> Serde<K> resolveKeySerde(Arguments<K, ?, ?> arguments) {
        final Class<Serde<K>> keySerdeClass = arguments.getKeySerdeClass();
        if (keySerdeClass != null) {
            /* Look for available Serdes on Serdes class as well. */
            return additionalSerdesConfig.serde(keySerdeClass);
        } else {
            @SuppressWarnings("unchecked")
            final Serde<K> defaultSerde = streamsConfig.defaultKeySerde();
            return defaultSerde;
        }
    }
}
