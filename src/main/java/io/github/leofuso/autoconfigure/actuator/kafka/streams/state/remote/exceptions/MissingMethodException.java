package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions;

/**
 * Indicates that a {@link java.lang.reflect.Method method} could not be located through in this particular
 * {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore store}.
 */
public class MissingMethodException extends RuntimeException {

    /**
     * Constructs a new {@link MissingMethodException exception}.
     *
     * @param storeReference {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore
     *                       store} reference.
     * @param methodName     {@link java.lang.reflect.Method method} name that's missing from the
     *                       {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore
     *                       store}.
     */
    public MissingMethodException(final String storeReference, final String methodName) {
        super("Store [" + storeReference + "] with method [" + methodName + "] isn't available.");
    }

}
