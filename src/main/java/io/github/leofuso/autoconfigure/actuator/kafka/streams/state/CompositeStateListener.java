package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;

/**
 * Interface for {@link KafkaStreams.StateListener listeners} that can combine source beans into a composite.
 */
public interface CompositeStateListener extends KafkaStreams.StateListener, StreamsBuilderFactoryBeanCustomizer {}
