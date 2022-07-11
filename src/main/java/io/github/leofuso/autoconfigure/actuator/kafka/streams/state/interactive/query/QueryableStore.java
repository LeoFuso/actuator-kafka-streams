package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import org.apache.kafka.streams.state.QueryableStoreType;

public interface QueryableStore {

    default boolean isCompatible(QueryableStoreType<?> type) {
        return type.getClass()
                   .equals(
                           type().getClass()
                   );
    }

    QueryableStoreType<?> type();

}
