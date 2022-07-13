package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.util.Assert;

public class Action<K, V, R> {

    private final String storeName;
    private final QueryableStoreType<?> queryableStoreType;
    private final String stringifiedKey;
    private final Class<Serde<K>> keySerdeClass;
    private final BiFunction<K, R, V> query;

    public Action(final String stringifiedKey,
                  final Class<Serde<K>> keySerdeClass,
                  final String storeName,
                  final QueryableStoreType<?> queryableStoreType,
                  final BiFunction<K, R, V> query) {

        Assert.hasText(stringifiedKey, "Attribute [stringifiedKey] cannot be blank");
        Assert.hasText(storeName, "Attribute [storeName] cannot be blank");
        Assert.notNull(queryableStoreType, "Attribute [queryableStoreType] cannot be null");
        Assert.notNull(query, "Attribute [query] cannot be null");

        this.stringifiedKey = stringifiedKey;
        this.storeName = storeName;
        this.keySerdeClass = keySerdeClass;
        this.queryableStoreType = queryableStoreType;
        this.query = query;
    }

    public static <K, V, R> Builder<K, V, R> performOn(String storeName, QueryableStoreType<?> queryableStoreType) {
        return new Builder<>(storeName, queryableStoreType);
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

    public BiFunction<K, R, V> getQuery() {
        return query;
    }

    public static class Builder<K, V, R> {

        private final String storeName;
        private final QueryableStoreType<?> queryableStoreType;
        private String stringifiedKey;
        private Class<Serde<K>> keySerdeClass;

        private Builder(String storeName, QueryableStoreType<?> queryableStoreType) {
            this.storeName = storeName;
            this.queryableStoreType = queryableStoreType;
        }

        public Builder<K, V, R> usingStringifiedKey(String stringifiedKey) {
            this.stringifiedKey = Objects.requireNonNull(stringifiedKey);
            return this;
        }

        public Builder<K, V, R> withKeySerdeClass(@Nullable String keySerdeClassName) throws ClassNotFoundException {
            if (keySerdeClassName == null) {
                return this;
            }

            @SuppressWarnings("unchecked")
            final Class<Serde<K>> keySerdeClass = (Class<Serde<K>>) Class.forName(keySerdeClassName);
            this.keySerdeClass = keySerdeClass;

            return this;
        }

        public Action<K, V, R> aQuery(BiFunction<K, R, V> query) {
            return new Action<>(
                    stringifiedKey,
                    keySerdeClass,
                    storeName,
                    queryableStoreType,
                    query
            );
        }
    }
}