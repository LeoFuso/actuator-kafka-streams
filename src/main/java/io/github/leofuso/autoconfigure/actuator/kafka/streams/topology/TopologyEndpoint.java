package io.github.leofuso.autoconfigure.actuator.kafka.streams.topology;

import java.util.Optional;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Actuator endpoint for topology description.
 */
@Endpoint(id = "topology")
public class TopologyEndpoint {

    /**
     * Default Topology response.
     */
    public static final String NO_TOPOLOGY_FOUND_MSG = "No topology found.";

    private final StreamsBuilderFactoryBean factoryBean;

    public TopologyEndpoint(final StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @ReadOperation
    public String topology() {
        return Optional.of(factoryBean)
                .filter(StreamsBuilderFactoryBean::isRunning)
                .map(StreamsBuilderFactoryBean::getTopology)
                .map(Topology::describe)
                .map(TopologyDescription::toString)
                .orElse(NO_TOPOLOGY_FOUND_MSG);
    }
}
