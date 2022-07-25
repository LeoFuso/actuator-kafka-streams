package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.core.convert.ConversionService;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.CompletableToListenableFutureAdapter;
import org.springframework.util.concurrent.ListenableFuture;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.AdditionalSerdesConfig;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions.UnableToLocateRemoteStoreException;

/**
 * Default implementation of {@link RemoteQuerySupport API}.
 */
public class DefaultRemoteQuerySupport implements RemoteQuerySupport {

    private final HostManager manager;

    private final ConversionService converter;

    private final StreamsConfig streamsConfig;
    private final AdditionalSerdesConfig additionalSerdesConfig;

    public DefaultRemoteQuerySupport(HostManager manager, ConversionService converter, Properties properties) {
        this.converter = Objects.requireNonNull(converter, "ConversionService [converter] is required");
        this.manager = Objects.requireNonNull(manager, "RemoteHostManager [manager] is required");
        Assert.state(properties != null, "Streams configuration properties must not be null.");
        this.streamsConfig = new StreamsConfig(properties);
        this.additionalSerdesConfig = new AdditionalSerdesConfig(properties);
    }

    @Override
    public <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName) {
        return manager.findHost(key, serializer, storeName);
    }

    @Override
    public <R extends RemoteStateStore> Optional<R> findStore(final String reference) {
        return manager.findStore(reference);
    }

    @Override
    public <R extends RemoteStateStore> Optional<R> findStore(HostInfo host, QueryableStoreType<?> storeType) {
        return manager.findStore(host, storeType);
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
                        final UnableToLocateRemoteStoreException exception =
                                new UnableToLocateRemoteStoreException(arguments);
                        return CompletableFuture.failedFuture(exception);
                    });
        }
    }

    @Override
    public <K, V, R extends RemoteStateStore> ListenableFuture<V> listenableInvoke(Arguments<K, V, R> arguments) {
        final CompletableFuture<V> future = invoke(arguments);
        return new CompletableToListenableFutureAdapter<>(future);
    }

    private <K> Serde<K> resolveKeySerde(Arguments<K, ?, ?> arguments) {
        final Class<Serde<K>> keySerdeClass = arguments.getKeySerdeClass();
        if (keySerdeClass != null) {
            /* Looks for available Serdes on Serdes class as well. */
            return additionalSerdesConfig.keySerde(keySerdeClass);
        } else {
            @SuppressWarnings("unchecked")
            final Serde<K> defaultSerde = streamsConfig.defaultKeySerde();
            return defaultSerde;
        }
    }
}
