package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.streams.KafkaStreams;

/**
 * A naive implementation of {@link CompositeStateListener} API.
 */
public class DefaultCompositeStateListener implements CompositeStateListener {

    private final Set<KafkaStreams.StateListener> listeners;

    /**
     * Creates a new instance of {@link CompositeStateListener}
     *
     * @param listeners a stream containing {@link org.apache.kafka.streams.KafkaStreams.StateListener listeners} to
     *                  delegate to.
     */
    public DefaultCompositeStateListener(final Stream<KafkaStreams.StateListener> listeners) {
        this.listeners = listeners.collect(Collectors.toSet());
    }

    @Override
    public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
        listeners.forEach(listener -> listener.onChange(newState, oldState));
    }

}
