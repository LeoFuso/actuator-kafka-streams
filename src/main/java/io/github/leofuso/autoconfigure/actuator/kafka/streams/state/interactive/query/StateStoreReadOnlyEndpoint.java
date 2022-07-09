package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.springframework.boot.actuate.endpoint.annotation.Selector.Match.ALL_REMAINING;

/**
 * Actuator endpoint for querying {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValue stores}.
 */
@Endpoint(id = "stateStoreReadOnly")
public class StateStoreReadOnlyEndpoint implements SmartInitializingSingleton {

    private final StreamsBuilderFactoryBean factory;
    private RemoteQueryableReadOnlyKeyValueStore<String, ?> store;

    public StateStoreReadOnlyEndpoint(final StreamsBuilderFactoryBean factory) {
        this.factory = factory;
    }

    /**
     * @param storeName {@link org.apache.kafka.streams.processor.StateStore StateStore} name to query.
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states, if
     * any.
     */
    @ReadOperation
    public Map<String, String> findByStoreNameAndKey(@Selector String storeName, @Selector(match = ALL_REMAINING) String key)
            throws NotBoundException, RemoteException {

        final boolean initialized = initialize();
        if (!initialized) {
            return Map.of();
        }

        final HostInfo hostInfo =
                store.queryMetadataForKey(key, storeName)
                     .map(KeyQueryMetadata::activeHost)
                     .orElse(null);

        if (hostInfo == null) {
            return Map.of();
        }

        return store
                .lookup(hostInfo)
                .flatMap(s -> s.findByKey(key, storeName))
                .map(value -> Map.of(key, value.toString()))
                .orElseGet(Map::of);
    }

    public boolean initialize() {

        if (store != null) {
            return true;
        }

        final Properties properties = factory.getStreamsConfiguration();
        final KafkaStreams streams = factory.getKafkaStreams();

        if (properties == null || streams == null) {
            return false;
        }

        final StreamsConfig streamsConfig = new StreamsConfig(properties);
        final RemoteQueryableReadOnlyKeyValueStore<String, ?> store = RemoteQueryableStore.readOnly(streamsConfig, streams);

        try {
            store.afterSingletonsInstantiated();
            this.store = store;
            return true;
        } catch (RuntimeException ex) {
            return false;
        }
    }

    @Override
    public void afterSingletonsInstantiated() {
        initialize();
    }

}
