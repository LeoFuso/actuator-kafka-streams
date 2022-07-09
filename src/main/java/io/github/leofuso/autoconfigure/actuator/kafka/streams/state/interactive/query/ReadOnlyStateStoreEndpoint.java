package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import javax.annotation.Nullable;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.core.convert.ConversionService;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

/**
 * Actuator endpoint for querying {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValue} stores.
 */
@Endpoint(id = "readyOnlyStateStore")
public class ReadOnlyStateStoreEndpoint {

    private final StreamsBuilderFactoryBean factory;
    private final ConversionService converter;

    private RemoteQueryableReadOnlyKeyValueStore store;

    public ReadOnlyStateStoreEndpoint(final StreamsBuilderFactoryBean factory, ConversionService conversionService) {
        this.factory = Objects.requireNonNull(factory, "Attribute [factory] is required.");
        this.converter = Objects.requireNonNull(conversionService, "Attribute [conversionService] is required.");
    }

    /**
     * @param storeName storeName of the {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore store} to query
     *                  to.
     * @param key       to query for.
     * @param keyType   the key type. Restricted by {@link org.apache.kafka.common.serialization.Serdes serdes}
     *                  supported types.
     * @param <K>       the key type. Restricted by {@link org.apache.kafka.common.serialization.Serdes serdes}
     *                  supported types.
     * @return the value associated with the key, if any. Will encapsulate
     * {@link Exception#getMessage() exception's messages} into resulting {@link Map map}.
     */
    @ReadOperation
    public <K> Map<String, String> find(@Selector String storeName, @Selector String key, @Nullable Class<K> keyType) {
        // TODO will need to create a WebExtension to support query parameters, sadly.
        try {
            if (keyType != null) {
                final K typedKey = converter.convert(key, keyType);
                return doFindByKey(typedKey, storeName);
            }
            return doFindByKey(key, storeName);
        } catch (RuntimeException ex) {
            return Map.of(key, ex.getMessage());
        }
    }

    public <K> Map<String, String> doFindByKey(K key, String storeName) {
        return ofNullable(store)
                .or(() -> {
                    try {
                        tryInitialize();
                        return of(store);
                    } catch (AlreadyBoundException | RemoteException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(store -> store.queryMetadataForKey(key, storeName))
                .map(KeyQueryMetadata::activeHost)
                .flatMap(info -> {
                    try {
                        return store.lookup(info);
                    } catch (NotBoundException | RemoteException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(remote -> remote.findByKey(key, storeName))
                .map(Object::toString)
                .map(value -> Map.of(key.toString(), value))
                .orElseGet(() -> Map.of(key.toString(), Strings.EMPTY));
    }

    public void tryInitialize() throws AlreadyBoundException, RemoteException {

        /* TODO This strategy will not work for remote queries. Need to fix that. */
        if (store != null) {
            return;
        }

        final Properties properties = factory.getStreamsConfiguration();
        final KafkaStreams streams = factory.getKafkaStreams();

        final StreamsConfig streamsConfig = new StreamsConfig(Objects.requireNonNull(properties));
        final RemoteQueryableReadOnlyKeyValueStore store = RemoteQueryableStore.readOnly(streamsConfig, streams);
        store.initialize();
        this.store = store;
    }

}
