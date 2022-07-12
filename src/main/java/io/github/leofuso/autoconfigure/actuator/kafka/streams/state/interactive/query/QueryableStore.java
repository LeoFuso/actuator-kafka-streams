package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.rmi.RemoteException;

import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * A {@link QueryableStore} encapsulates a {@link org.apache.kafka.streams.processor.StateStore store} capable of
 * receiving read-only queries, for specific keys.
 */
public interface QueryableStore {

    /**
     * @param type to compare against.
     * @return either or not the given {@link QueryableStoreType type} is compatible with this {@link QueryableStore}
     * instance.
     */
    default boolean isCompatible(QueryableStoreType<?> type) throws RemoteException {
        return type.getClass()
                   .equals(
                           type().getClass()
                   );
    }

    /**
     * @return the {@link QueryableStoreType} associated with this {@link QueryableStore}.
     */
    QueryableStoreType<?> type() throws RemoteException;

}
