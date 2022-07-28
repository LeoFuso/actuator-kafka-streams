package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Interface for {@link KafkaStreams.StateListener listeners} that can combine source beans into a composite.
 */
public interface CompositeStateListener extends KafkaStreams.StateListener {}
