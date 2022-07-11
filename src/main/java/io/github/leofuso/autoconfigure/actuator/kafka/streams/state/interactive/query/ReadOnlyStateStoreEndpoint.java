package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.ConverterNotFoundException;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote.RemoteQueryableReadOnlyKeyValueStore;

/**
 * Actuator endpoint for querying {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValue} stores.
 */
@Endpoint(id = "readonlystatestore")
public class ReadOnlyStateStoreEndpoint {

    private static final String ERROR_MESSAGE_KEY = "message";

    private final InteractiveQueryService service;
    private final ConversionService converter;

    public ReadOnlyStateStoreEndpoint(final InteractiveQueryService service, final ConversionService converter) {
        this.service = service;
        this.converter = converter;
    }


    /**
     * Query for a value associated with given key and store.
     *
     * @param storeName of the {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore queryable store}.
     * @param key       to query for.
     * @param keyClass  the key class. Restricted to supported
     *                  {@link org.apache.kafka.common.serialization.Serdes serdes} types.
     * @return the value associated with the key, if any. Will encapsulate eventual
     * {@link Exception#getMessage() exception's messages} into a response object.
     *
     * @implNote Due to the nature of the query Api this is a relative expensive operation and should be invoked with
     * care. All disposable objects will only persist during the lifecycle of this query to save on resources.
     */
    @ReadOperation
    public Map<String, String> find(@Selector String storeName,
                                    @Selector String key,
                                    @Nullable String keyClass,
                                    @Nullable String serdeClass) {

        try {
            if(serdeClass != null) {
                final Class<?> actualSerdeClass = resolveSerdeClass(serdeClass);
                final Object resolvedKey = resolveKeyUsingKeyClass(key, keyClass);
                return doFindByKey(resolvedKey, actualSerdeClass, storeName);
            }
            if (keyClass != null) {
                final Object resolvedKey = resolveKeyUsingKeyClass(key, keyClass);
                return doFindByKey(resolvedKey, storeName);
            }
            return doFindByKey(key, storeName);
        } catch (RuntimeException ex) {
            return Map.of(ERROR_MESSAGE_KEY, ex.getMessage());
        }
    }


    public Object resolveKeyUsingKeyClass(String rawKey, String keyClass) {
        try {
            final Class<?> actualKeyClass = Class.forName(keyClass);
            return converter.convert(rawKey, actualKeyClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (ConverterNotFoundException ex) {
            final String message =
                    "Please make sure the right Converter is available in the classpath." +
                            " Alternatively, you can implement your own." + ex.getMessage();
            throw new RuntimeException(message);
        }
    }

    public Class<?> resolveSerdeClass(String serdeClass) {
        try {
            return Class.forName(serdeClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public <K> Map<String, String> doFindByKey(K key, String storeName) {

        final Optional<RemoteQueryableReadOnlyKeyValueStore> store =
                service.store(key, storeName, QueryableStoreTypes.keyValueStore());

        return store.map(s -> s.findByKey(key, storeName))
                    .map(Object::toString)
                    .map(value -> Map.of(key.toString(), value))
                    .orElseGet(() -> Map.of(key.toString(), Strings.EMPTY));
    }

    public <K, S extends Serde<K>> Map<String, String> doFindByKey(K key, Class<S> serdeClass, String storeName) {

        final Optional<RemoteQueryableReadOnlyKeyValueStore> store =
                service.store(key, serdeClass, storeName, QueryableStoreTypes.keyValueStore());

        return store.map(s -> s.findByKey(key, storeName))
                    .map(Object::toString)
                    .map(value -> Map.of(key.toString(), value))
                    .orElseGet(() -> Map.of(key.toString(), Strings.EMPTY));
    }

}
