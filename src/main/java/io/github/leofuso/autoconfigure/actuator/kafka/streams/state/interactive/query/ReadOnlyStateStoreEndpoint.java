package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import javax.annotation.Nullable;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.ConverterNotFoundException;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.ReadOnlyStateStoreEndpoint.ENDPOINT;

/**
 * Actuator endpoint for querying {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValue} stores.
 */
@Endpoint(id = ENDPOINT)
public class ReadOnlyStateStoreEndpoint {

    public static final String ENDPOINT = "readonlystatestore";
    private static final String ERROR_MESSAGE_KEY = "message";

    private final BeanFactory factory;

    private RemoteQueryableReadOnlyKeyValueStore readOnlyKeyValueStore;

    public ReadOnlyStateStoreEndpoint(final BeanFactory factory) {
        this.factory = Objects.requireNonNull(factory, "BeanFactory [factory] is required.");
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
    public Map<String, String> find(@Selector String storeName, @Selector String key, @Nullable String keyClass) {

        try {
            initialize();
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

            /* Lazy invocation */
            final ConversionService converter = factory.getBean(ConversionService.class);
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

    public <K> Map<String, String> doFindByKey(K key, String storeName) {
        return Optional.ofNullable(readOnlyKeyValueStore)
                       .or(() -> {
                           try {
                               initialize();
                               return Optional.of(readOnlyKeyValueStore);
                           } catch (AlreadyBoundException | RemoteException e) {
                               throw new RuntimeException(e);
                           }
                       })
                       .flatMap(store -> store.queryMetadataForKey(key, storeName))
                       .map(KeyQueryMetadata::activeHost)
                       .flatMap(info -> {
                           try {
                               return readOnlyKeyValueStore.lookup(info);
                           } catch (NotBoundException | RemoteException e) {
                               throw new RuntimeException(e);
                           }
                       })
                       .flatMap(remote -> remote.findByKey(key, storeName))
                       .map(Object::toString)
                       .map(value -> Map.of(key.toString(), value))
                       .orElseGet(() -> Map.of(key.toString(), Strings.EMPTY));
    }

    /**
     * We try to initialize the {@link RemoteQueryableStore store} lazily. It's possible that this operation will fail
     * indefinitely due to a simple miss configuration, or any other factor that can make a
     * {@link org.apache.kafka.streams.KafkaStreams Stream} unavailable.
     * <p>
     * Since most of those validations only happens after or during the App initialization, we cannot associate the
     * lifecycle of this {@link Endpoint endpoint} to it.
     */
    public void initialize() {
        if (readOnlyKeyValueStore != null) {
            return;
        }
        this.readOnlyKeyValueStore = RemoteQueryableStore.localReadOnly(factory);
    }

}
