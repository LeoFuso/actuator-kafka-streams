package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.function.Function;

import org.apache.kafka.streams.state.QueryableStoreType;

import io.grpc.ManagedChannelBuilder;

/**
 * The stub responsible for all gRPC {@link RemoteStateStore store} invocations.
 */
public interface RemoteStateStoreStub extends RemoteStateStore {

    @Override
    default boolean isCompatible(QueryableStoreType<?> type) {
        return false;
    }

    /**
     * Customizes this instance.
     * @param configuration available customization to be applied to this instance.
     */
    void configure(Function<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> configuration);

    /**
     * Initializes this stub. Should be invoked before made available to receive
     * actual requests, and after all configurations were applied.
     */
    void initialize();

    /**
     * Starts a shutdown process for the {@link io.grpc.Channel channel} associated with this
     * stub.
     */
    void shutdown();

}
