package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.AdditionalSerdesConfig;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.Arguments;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteKeyValueStateStore;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteQuerySupport;

import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

/**
 * Actuator endpoint for querying
 * {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteKeyValueStateStore stores}.
 */
@Endpoint(id = "readonlystatestore")
public class ReadOnlyStateStoreEndpoint {

    private static final String ERROR_MESSAGE_KEY = "message";

    private final RemoteQuerySupport support;
    private final ObjectMapper mapper;

    /**
     * Constructs a new ReadOnlyStateStoreEndpoint instance.
     *
     * @param support to delegate the queries to.
     * @param mapper to build a JsonNode from query result, if needed.
     */
    public ReadOnlyStateStoreEndpoint(RemoteQuerySupport support, ObjectMapper mapper) {
        this.support = Objects.requireNonNull(support, "RemoteQuerySupport [support] is required.");
        this.mapper = Objects.requireNonNull(mapper, "ObjectMapper [mapper] is required.");
    }


    /**
     * Query for a value associated with given key and store.
     *
     * @param store of the {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore queryable store}.
     * @param key   to query for.
     * @param serde the key class. Restricted to supported {@link org.apache.kafka.common.serialization.Serdes serdes}
     *              types or {@link AdditionalSerdesConfig additional ones};
     * @param <K>   the key type.
     * @param <V>   the value type.
     * @return the value associated with the key, if any. Will encapsulate eventual
     * {@link Exception#getMessage() exception's messages} into a response object.
     */
    @ReadOperation
    public <K, V> JsonNode find(@Selector String store, @Selector String key, @Nullable String serde) {

        final BiFunction<K, RemoteKeyValueStateStore, CompletableFuture<V>> invocation =
                (k, s) -> s.findOne(k, store);

        try {

            final Arguments<K, V, RemoteKeyValueStateStore> arguments =
                    Arguments.perform(invocation)
                             .passing(key, serde)
                             .on(store, keyValueStore());

            final CompletableFuture<V> invocationFuture = support.invoke(arguments)
                                                                 .orTimeout(10, TimeUnit.SECONDS);

            /* Try to find a way of making this endpoint Reactive? */
            final Object value = invocationFuture.get();

            final ObjectNode object = mapper.createObjectNode();
            object.put("key", key);

            final String stringifiedValue =
                    Optional.ofNullable(value)
                            .map(Object::toString)
                            .orElse(Strings.EMPTY);
            try {

                final JsonNode valueNode = mapper.readTree(stringifiedValue);
                object.set("value", valueNode);
                return object;

            } catch (JsonParseException ignored) {
                object.put("value", stringifiedValue);
                return object;
            }

        } catch (Exception ex) {
            final Throwable cause = ex.getCause();
            final String message = Optional
                    .ofNullable(cause)
                    .map(Throwable::toString)
                    .map(nested -> ex.getMessage() + "; nested exception is " + nested)
                    .orElseGet(ex::toString);

            final Map<String, String> errorMessageTree = Map.of(ERROR_MESSAGE_KEY, message);
            return mapper.valueToTree(errorMessageTree);
        }
    }
}
