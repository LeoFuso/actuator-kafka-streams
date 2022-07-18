package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc;

import org.springframework.core.Ordered;

import io.grpc.ServerBuilder;

/**
 * A Configurer that can be used to configure a {@link io.grpc.Server server} before its creation.
 */
public interface GrpcServerConfigurer extends Ordered {

    /**
     * @param builder that can receive new configurations.
     * @return the configured {@link ServerBuilder builder}.
     */
    ServerBuilder<?> configure(ServerBuilder<?> builder);

}
