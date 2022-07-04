package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import org.apache.kafka.streams.processor.StateRestoreListener;

/**
 *  Interface for {@link StateRestoreListener listeners} that can combine source beans into a
 *  composite.
 */
public interface CompositeStateRestoreListener extends StateRestoreListener {}
