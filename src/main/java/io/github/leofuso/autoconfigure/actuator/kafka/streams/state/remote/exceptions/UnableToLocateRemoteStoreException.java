package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.Arguments;

/**
 * Indicates that a specific
 * {@link io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore store} wasn't found.
 * Probably due to miss configuration or premature invocation issues.
 */
public class UnableToLocateRemoteStoreException extends RuntimeException {

    /**
     * Create a new {@link UnableToLocateRemoteStoreException exception} for given arguments.
     *
     * @param arguments used to express the queried missing store.
     */
    public UnableToLocateRemoteStoreException(Arguments<?, ?, ?> arguments) {
        super(
                String.format(
                        "Remote %s [%s], owning [%s] is not available. Maybe it hasn't started yet?",
                        arguments.getQueryableStoreType()
                                 .getClass()
                                 .getSimpleName(),
                        arguments.getStoreName(),
                        arguments.getStringifiedKey())
        );
    }
}
