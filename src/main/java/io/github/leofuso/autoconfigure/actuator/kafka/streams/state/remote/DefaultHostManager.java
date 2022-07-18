package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.GrpcChannelConfigurer;


/**
 * Default implementation of the {@link HostManager manager} API.
 */
public class DefaultHostManager implements HostManager {

    private final ConcurrentHashMap<HostInfo, RemoteStateStore> stores;

    private final StreamsBuilderFactoryBean factory;

    private final Set<RemoteStateStore> supported;

    private final Set<GrpcChannelConfigurer> configuration;


    public DefaultHostManager(StreamsBuilderFactoryBean factory,
                              Stream<RemoteStateStore> supported,
                              Stream<GrpcChannelConfigurer> configuration) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
        this.supported = supported.collect(Collectors.toSet());
        this.configuration = configuration.collect(Collectors.toSet());
        this.stores = new ConcurrentHashMap<>();
    }

    @Override
    public <K> Optional<HostInfo> findHost(final K key, final Serializer<K> serializer, final String storeName) {
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
        for (RemoteStateStore supported : supported) {
            final String storeReference = supported.reference();
            final boolean sameReference = storeReference.equals(reference);
            if (sameReference) {
                @SuppressWarnings("unchecked")
                final R store = (R) supported;
                return Optional.of(store);
            }
        }
        return Optional.empty();
    }

    @Override
    public <R extends RemoteStateStore> Optional<R> findStore(HostInfo host, QueryableStoreType<?> storeType) {
        for (RemoteStateStore supported : supported) {

            final boolean incompatible = !supported.isCompatible(storeType);
            if (incompatible) {
                continue;
            }

            final RemoteStateStore store = stores.get(host);
            if (store != null) {
                @SuppressWarnings("unchecked")
                final R stub = (R) store;
                return Optional.of(stub);
            }

            final R remote = supported.stub(host);
            if (remote instanceof RemoteStateStoreStub) {
                final RemoteStateStoreStub stub = (RemoteStateStoreStub) remote;
                configuration.forEach(config -> stub.configure(config::configure));
                stub.initialize();
            }

            stores.put(host, remote);
            return Optional.of(remote);
        }
        return Optional.empty();
    }

    @Override
    public void shutdown() throws InterruptedException {
        for (RemoteStateStore store : stores.values()) {
            if (store instanceof RemoteStateStoreStub) {
                final RemoteStateStoreStub stub = (RemoteStateStoreStub) store;
                stub.shutdown();
            }
        }
    }
}
