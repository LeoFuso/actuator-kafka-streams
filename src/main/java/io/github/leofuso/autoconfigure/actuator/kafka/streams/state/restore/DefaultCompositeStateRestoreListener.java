package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

/**
 * Default implementation of {@link CompositeStateRestoreListener composite listener}.
 */
public class DefaultCompositeStateRestoreListener implements CompositeStateRestoreListener {

    private final Set<StateRestoreListener> listeners;

    public DefaultCompositeStateRestoreListener(final Stream<StateRestoreListener> listeners) {
        this.listeners = listeners.collect(Collectors.toSet());
    }

    @Override
    public void onRestoreStart(final TopicPartition topicPartition,
                               final String storeName,
                               final long startingOffset,
                               final long endingOffset) {
        listeners.forEach(listener -> listener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset));
    }

    @Override
    public void onBatchRestored(final TopicPartition topicPartition,
                                final String storeName,
                                final long batchEndOffset,
                                final long numRestored) {
        listeners.forEach(listener -> listener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored));
    }

    @Override
    public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
        listeners.forEach(listener -> listener.onRestoreEnd(topicPartition, storeName, totalRestored));
    }
}
