package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions;

/**
 * Indicates that a {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore store}
 * could not be located through
 * {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteQuerySupport#findStore(String)}
 * method.
 */
public class MissingStoreException extends RuntimeException {

    /**
     * Constructs a new {@link MissingStoreException exception}.
     *
     * @param storeReference the bean reference of the
     *                       {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore
     *                       store.}
     */
    public MissingStoreException(final String storeReference) {
        super("Store [" + storeReference + "] bean isn't available.");
    }

}
