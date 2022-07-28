package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import org.apache.kafka.streams.processor.StateRestoreListener;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;

/**
 *  Interface for {@link StateRestoreListener listeners} that can combine source beans into a
 *  composite.
 */
public interface CompositeStateRestoreListener extends StateRestoreListener, StreamsBuilderFactoryBeanCustomizer {}
