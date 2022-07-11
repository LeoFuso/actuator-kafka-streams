package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.AdditionalSerdesConfig;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableStore;

public class InteractiveQueryServiceImpl implements InteractiveQueryService {

    private final StreamsBuilderFactoryBean factory;
    private final Set<RemoteQueryableStore> remoteStores;

    public InteractiveQueryServiceImpl(StreamsBuilderFactoryBean factory, Set<RemoteQueryableStore> remoteStores) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        this.remoteStores = Optional.ofNullable(remoteStores)
                                    .orElseGet(Set::of);
    }

    @Override
    public <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName) {
        return Optional.ofNullable(factory.getKafkaStreams())
                       .map(streams -> streams.queryMetadataForKey(storeName, key, serializer))
                       .filter(m -> !KeyQueryMetadata.NOT_AVAILABLE.equals(m))
                       .map(KeyQueryMetadata::activeHost);
    }

    @Override
    public <K, T, R extends RemoteQueryableStore> Optional<R> store(K key, String store, QueryableStoreType<T> type) {
        try(Serde<K> serde = resolveSerde(key)) {
            final Serializer<K> serializer = serde.serializer();
            return findHost(key, serializer, store)
                    .flatMap(info -> store(info, type));
        }
    }

    @Override
    public <K, T, S extends Serde<K>, R extends RemoteQueryableStore> Optional<R> store(K key, Class<S> serdeClass, String store, QueryableStoreType<T> type) {
        try(Serde<K> serde = resolveSerde(serdeClass)) {
            final Serializer<K> serializer = serde.serializer();
            return findHost(key, serializer, store)
                    .flatMap(info -> store(info, type));
        }
    }

    @Override
    public <T, S extends RemoteQueryableStore> Optional<S> store(HostInfo host, QueryableStoreType<T> type) {
        return remoteStores.stream()
                           .filter(store -> store.isCompatible(type))
                           .findAny()
                           .map(store -> store.stub(host));
    }


    @SuppressWarnings("unchecked")
    private <K> Serde<K> resolveSerde(K key) {
        return Optional.ofNullable(factory.getStreamsConfiguration())
                       .map(StreamsConfig::new)
                       .map(streamsConfig -> {

                           final Serde<K> serde = streamsConfig.defaultKeySerde();
                           final Class<?> serdeType = AdditionalSerdesConfig.serdeType(serde);
                           final boolean isAssignableFrom = key.getClass()
                                                               .isAssignableFrom(serdeType);
                           if (isAssignableFrom) {
                               return serde;
                           }

                           throw new IllegalArgumentException("Provided Key is not assignable from default key Serde");
                       })
                       .orElseThrow();

    }

    private <K, S extends Serde<K>> Serde<K> resolveSerde(Class<S> serdeClass) {
        return Optional.ofNullable(factory.getStreamsConfiguration())
                       .map(AdditionalSerdesConfig::new)
                       .map(config -> config.serde(serdeClass))
                       .orElseThrow();

    }

}
