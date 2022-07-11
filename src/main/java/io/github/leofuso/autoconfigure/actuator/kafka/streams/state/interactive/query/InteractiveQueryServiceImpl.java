package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableStore;

public class InteractiveQueryServiceImpl implements InteractiveQueryService {

    private final StreamsBuilderFactoryBean factory;
    private final Set<RemoteQueryableStore> remoteStores;

    public InteractiveQueryServiceImpl(StreamsBuilderFactoryBean factory, Set<RemoteQueryableStore> remoteStores) {
        this.factory = factory;
        this.remoteStores = remoteStores;
    }

    @Override
    public <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName) {
        return Optional.ofNullable(factory.getKafkaStreams())
                       .map(streams -> streams.queryMetadataForKey(storeName, key, serializer))
                       .filter(m -> !KeyQueryMetadata.NOT_AVAILABLE.equals(m))
                       .map(KeyQueryMetadata::activeHost);
    }

    @Override
    public <T, S extends RemoteQueryableStore> Optional<S> store(HostInfo host, QueryableStoreType<T> type) {
        return remoteStores.stream()
                           .filter(store -> store.isCompatible(type))
                           .findAny()
                           .map(store -> store.stub(host));
    }

}
