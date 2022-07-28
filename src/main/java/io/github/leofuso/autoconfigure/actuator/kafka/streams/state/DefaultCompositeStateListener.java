package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.apache.kafka.streams.KafkaStreams.State;
import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * A naive implementation of {@link CompositeStateListener} API.
 */
public class DefaultCompositeStateListener implements CompositeStateListener, SmartInitializingSingleton {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCompositeStateListener.class);

    private final ObjectProvider<StateListener> listeners;

    /**
     * Creates a new instance of {@link CompositeStateListener}
     *
     * @param listeners a stream containing {@link StateListener listeners} to delegate to.
     */
    public DefaultCompositeStateListener(ObjectProvider<StateListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void onChange(final State newState, final State oldState) {
        listeners.forEach(listener -> listener.onChange(newState, oldState));
    }

    @Override
    public void afterSingletonsInstantiated() {
        final Set<StateListener> listeners =
                this.listeners.orderedStream()
                              .collect(Collectors.toSet());

        final int count = listeners.size();
        logger.info(
                "A DefaultCompositeStateListener, composed of {} listeners, is listening on KafkaStreams events.",
                count
        );
        if (logger.isDebugEnabled()) {
            this.listeners.forEach(listener -> {
                final Class<? extends StateListener> aClass = listener.getClass();
                final String name = aClass.getSimpleName();
                logger.debug("An instance of [{}] is listening on KafkaStreams events.", name);
            });
        }
    }

    @Override
    public void customize(final StreamsBuilderFactoryBean factoryBean) {
        factoryBean.setStateListener(this);
    }
}
