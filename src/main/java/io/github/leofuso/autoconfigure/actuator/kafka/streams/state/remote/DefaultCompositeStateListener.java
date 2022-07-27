package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.KafkaStreams.State;
import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * A naive implementation of {@link CompositeStateListener} API.
 */
public class DefaultCompositeStateListener implements CompositeStateListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCompositeStateListener.class);

    private final Set<StateListener> listeners;

    /**
     * Creates a new instance of {@link CompositeStateListener}
     *
     * @param listeners a stream containing {@link StateListener listeners} to delegate to.
     */
    public DefaultCompositeStateListener(final Stream<StateListener> listeners) {
        this.listeners = listeners.collect(Collectors.toSet());
        final int count = this.listeners.size();
        logger.info(
                "A DefaultCompositeStateListener, composed of {} listeners, is listening on KafkaStreams events.",
                count
        );
        if (logger.isDebugEnabled()) {
            listeners.forEach(listener -> {
                final Class<? extends StateListener> aClass = listener.getClass();
                final String name = aClass.getSimpleName();
                logger.debug("An instance of [{}] is listening on KafkaStreams events.", name);
            });
        }
    }

    @Override
    public void onChange(final State newState, final State oldState) {
        listeners.forEach(listener -> listener.onChange(newState, oldState));
    }
}
