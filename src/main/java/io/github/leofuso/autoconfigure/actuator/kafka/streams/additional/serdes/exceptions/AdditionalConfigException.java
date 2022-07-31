package io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.exceptions;

/**
 * Thrown if the user supplies an invalid configuration for additional config properties.
 */
public class AdditionalConfigException extends RuntimeException {

    /**
     * Constructs a new {@link AdditionalConfigException} with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this runtime exception's detail message.
     *
     * @param  message the detail message.
     * @param  cause the cause. (A {@code null} value is
     *         permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public AdditionalConfigException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
