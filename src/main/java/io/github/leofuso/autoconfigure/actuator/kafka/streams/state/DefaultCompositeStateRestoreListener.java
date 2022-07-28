package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * Default implementation of {@link CompositeStateRestoreListener composite listener}.
 */
public class DefaultCompositeStateRestoreListener implements CompositeStateRestoreListener, SmartInitializingSingleton {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCompositeStateRestoreListener.class);

    private final ObjectProvider<StateRestoreListener> listeners;

    public DefaultCompositeStateRestoreListener(final ObjectProvider<StateRestoreListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void onRestoreStart(TopicPartition tp, String storeName, long startingOffset, long endingOffset) {
        listeners.forEach(listener -> listener.onRestoreStart(tp, storeName, startingOffset, endingOffset));
    }

    @Override
    public void onBatchRestored(TopicPartition tp, String storeName, long batchEndOffset, long numRestored) {
        listeners.forEach(listener -> listener.onBatchRestored(tp, storeName, batchEndOffset, numRestored));
    }

    @Override
    public void onRestoreEnd(final TopicPartition tp, final String storeName, final long totalRestored) {
        listeners.forEach(listener -> listener.onRestoreEnd(tp, storeName, totalRestored));
    }

    @Override
    public void afterSingletonsInstantiated() {
        final Set<StateRestoreListener> listeners =
                this.listeners.orderedStream()
                              .collect(Collectors.toSet());

        final int count = listeners.size();
        logger.info(
                "A DefaultCompositeStateRestoreListener, composed of {} listeners, is listening on StateRestore events.",
                count
        );
        if (logger.isDebugEnabled()) {
            this.listeners.forEach(listener -> {
                final Class<? extends StateRestoreListener> aClass = listener.getClass();
                final String name = aClass.getSimpleName();
                logger.debug("An instance of [{}] is listening on StateRestore events.", name);
            });
        }
    }
}

