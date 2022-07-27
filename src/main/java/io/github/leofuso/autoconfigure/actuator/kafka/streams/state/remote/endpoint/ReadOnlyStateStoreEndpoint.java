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

    /**
     * Constructs a new ReadOnlyStateStoreEndpoint instance.
     * @param support to delegate the queries to.
     */
    public ReadOnlyStateStoreEndpoint(final RemoteQuerySupport support) {
        this.support = Objects.requireNonNull(support, "RemoteQuerySupport [support] is required.");
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
    public <K, V> Map<String, String> find(@Selector String store, @Selector String key, @Nullable String serde) {

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
            final V v = invocationFuture.get();
            return Optional.ofNullable(v)
                           .map(value -> Map.of(key, value.toString()))
                           .orElseGet(() -> Map.of(key, Strings.EMPTY));

        } catch (Exception ex) {
            final Throwable cause = ex.getCause();
            final String message = Optional
                    .ofNullable(cause)
                    .map(Throwable::toString)
                    .map(nested -> ex.getMessage() + "; nested exception is " + nested)
                    .orElseGet(ex::toString);

            return Map.of(ERROR_MESSAGE_KEY, message);
        }
    }
}
