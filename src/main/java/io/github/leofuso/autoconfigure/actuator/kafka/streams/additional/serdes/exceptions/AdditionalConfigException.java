package io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.exceptions;

/**
 * Thrown if the user supplies an invalid configuration for additional config properties.
 */
public class AdditionalConfigException extends RuntimeException {

    public AdditionalConfigException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
