package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.time.Duration;
import java.util.function.Function;

import org.apache.kafka.streams.state.QueryableStoreType;

import io.grpc.ManagedChannelBuilder;

public interface RemoteStateStoreStub extends RemoteStateStore {

    @Override
    default boolean isCompatible(QueryableStoreType<?> type) {
        return false;
    }

    void configure(Function<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> configuration);

    void initialize();

    void shutdown() throws InterruptedException;

    void shutdown(Duration timeout) throws InterruptedException;

}
