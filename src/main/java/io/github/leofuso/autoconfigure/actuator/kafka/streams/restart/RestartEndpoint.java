package io.github.leofuso.autoconfigure.actuator.kafka.streams.restart;

import java.util.Objects;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Actuator endpoint for restarting a {@link org.apache.kafka.streams.KafkaStreams KafkaStreams} App.
 */
@Endpoint(id = "restart")
public class RestartEndpoint {

    /**
     * To delegate the restart action to.
     */
    private final StreamsBuilderFactoryBean factory;

    /**
     * Creates a new {@link RestartEndpoint} instance.
     *
     * @param factory to delegate the restart action to.
     */
    public RestartEndpoint(final StreamsBuilderFactoryBean factory) {
        this.factory = Objects.requireNonNull(factory, "StreamsBuilderFactoryBean [factory] is required.");
    }

    /**
     * Will commence a restart process by first trying to stop a possible, but not necessarily, running KafkaStreams
     * instance, and them starting another one on its place.
     *
     * <p><strong>WARNING</strong>: This utility can leave the application in an unrecoverable state.</p>
     */
    @WriteOperation
    public void restart() {
        factory.stop();
        factory.start();
    }
}
