package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc;

import org.springframework.core.Ordered;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;

/**
 * A Configurer that can be used to configure a {@link io.grpc.Channel channel} before its creation.
 */
public interface GrpcChannelConfigurer extends Ordered {

    /**
     * @param builder that can receive new configurations.
     * @return the configured {@link ManagedChannelBuilder builder}.
     */
    ManagedChannelBuilder<?> configure(ManagedChannelBuilder<?> builder);

}
