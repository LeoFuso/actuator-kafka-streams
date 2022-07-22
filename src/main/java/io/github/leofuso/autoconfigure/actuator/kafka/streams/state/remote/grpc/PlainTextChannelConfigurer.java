package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc;

import io.grpc.ManagedChannelBuilder;

/**
 * A {@link GrpcChannelConfigurer channel configurer} that configures a <code>plain text authentication</code>
 * strategy.
 */
public class PlainTextChannelConfigurer implements GrpcChannelConfigurer {

    @Override
    public ManagedChannelBuilder<?> configure(final ManagedChannelBuilder<?> builder) {
        return builder.usePlaintext();
    }

    @Override
    public int getOrder() {
        /* Lowest priority */
        return Integer.MIN_VALUE;
    }

}
