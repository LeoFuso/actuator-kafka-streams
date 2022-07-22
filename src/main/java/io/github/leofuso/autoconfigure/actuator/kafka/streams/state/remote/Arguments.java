package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.util.Assert;

public class Arguments<K, V, R> {

    private final String storeName;
    private final QueryableStoreType<?> queryableStoreType;
    private final String stringifiedKey;
    private final Class<Serde<K>> keySerdeClass;
    private final BiFunction<K, R, CompletableFuture<V>> invocation;

    public Arguments(final BiFunction<K, R, CompletableFuture<V>> invocation,
                     final String stringifiedKey,
                     final Class<Serde<K>> keySerdeClass,
                     final String storeName,
                     final QueryableStoreType<?> queryableStoreType) {

        Assert.notNull(invocation, "Attribute [query] cannot be null");
        Assert.hasText(stringifiedKey, "Attribute [stringifiedKey] cannot be blank");
        Assert.hasText(storeName, "Attribute [storeName] cannot be blank");
        Assert.notNull(queryableStoreType, "Attribute [queryableStoreType] cannot be null");

        this.invocation = invocation;
        this.stringifiedKey = stringifiedKey;
        this.keySerdeClass = keySerdeClass;
        this.storeName = storeName;
        this.queryableStoreType = queryableStoreType;
    }

    public static <K, V, R> Builder<K, V, R> perform(BiFunction<K, R, CompletableFuture<V>> invocation) {
        return new Builder<>(invocation);
    }

    public String getStringifiedKey() {
        return stringifiedKey;
    }

    @Nullable
    public Class<Serde<K>> getKeySerdeClass() {
        return keySerdeClass;
    }

    public String getStoreName() {
        return storeName;
    }

    public QueryableStoreType<?> getQueryableStoreType() {
        return queryableStoreType;
    }

    public BiFunction<K, R, CompletableFuture<V>> getInvocation() {
        return invocation;
    }

    public static class Builder<K, V, R> {

        private final BiFunction<K, R, CompletableFuture<V>> invocation;

        private String stringifiedKey;
        private Class<Serde<K>> keySerdeClass;

        public Builder(final BiFunction<K, R, CompletableFuture<V>> invocation) {
            this.invocation = Objects.requireNonNull(invocation);
        }


        public Builder<K, V, R> passing(String stringifiedKey) {
            this.stringifiedKey = Objects.requireNonNull(stringifiedKey);
            return this;
        }

        public Builder<K, V, R> passing(String stringifiedKey, @Nullable String keySerdeClassName) {
            this.stringifiedKey = Objects.requireNonNull(stringifiedKey);
            if (keySerdeClassName == null) {
                return this;
            }
            try {
                @SuppressWarnings("unchecked")
                final Class<Serde<K>> keySerdeClass = (Class<Serde<K>>) Class.forName(keySerdeClassName);
                this.keySerdeClass = keySerdeClass;
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("ClassNotFoundException: " + e.getMessage(), e);
            }
            return this;
        }


        public Arguments<K, V, R> on(String storeName, QueryableStoreType<?> queryableStoreType) {
            return new Arguments<>(
                    this.invocation,
                    stringifiedKey,
                    keySerdeClass,
                    storeName,
                    queryableStoreType
            );
        }
    }
}