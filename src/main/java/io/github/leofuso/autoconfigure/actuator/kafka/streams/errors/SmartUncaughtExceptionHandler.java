package io.github.leofuso.autoconfigure.actuator.kafka.streams.errors;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;

/**
 * A {@link SmartUncaughtExceptionHandler} can be configured and decide either or not should recover from a thrown
 * {@link Exception exception}.
 */
@SuppressWarnings("unused")
public class SmartUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SmartUncaughtExceptionHandler.class);

    /**
     * Exceptions that may be worth to try to recover from.
     */
    private final Set<Class<? extends Throwable>> naiveRecoverableInclusion = Set.of(
            NetworkException.class, // Usually a Broker reelection or something punctual */
            TimeoutException.class, // Again, Network related.
            RetriableException.class // The name give it away, didn't it?
    );

    /**
     * Exceptions that we'll never try to recover from.
     */
    private final Set<Class<? extends Throwable>> obligatoryUnrecoverableInclusion = Set.of(
            IllegalArgumentException.class, // Not by choice. Hard limit of the library.
            IllegalStateException.class // Not by choice. Hard limit of the library.
    );

    private final Set<UncaughtExceptionDecider> deciders;

    /**
     * Creates a new instance of SmartUncaughtExceptionHandler.
     */
    public SmartUncaughtExceptionHandler() {
        this(Stream.empty());
    }

    /**
     * Creates a new instance of SmartUncaughtExceptionHandler.
     *
     * @param deciders used to decide either or not a
     *                 {@link org.apache.kafka.streams.processor.internals.StreamThread thread} can be replaced.
     */
    public SmartUncaughtExceptionHandler(final Stream<UncaughtExceptionDecider> deciders) {
        Objects.requireNonNull(deciders, "Stream of UncaughtExceptionDecider [deciders] is required");
        this.deciders = deciders.sorted(Comparator.comparing(Ordered::getOrder))
                                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {

        final Class<? extends Throwable> exceptionClass = exception.getClass();
        for (Class<? extends Throwable> recoverable : naiveRecoverableInclusion) {
            if (recoverable.isAssignableFrom(exceptionClass)) {
                logger.error("Applying [replace-thread] strategy from recoverable inclusions.", exception);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }

        for (Class<? extends Throwable> nonRecoverable : obligatoryUnrecoverableInclusion) {
            if (nonRecoverable.isAssignableFrom(exceptionClass)) {
                logger.error("Applying [shutdown-client] strategy from unrecoverable inclusions.", exception);
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }

        for (UncaughtExceptionDecider decider : deciders) {
            if (decider.shouldRecover(exception)) {
                final Class<? extends UncaughtExceptionDecider> deciderClass = decider.getClass();
                final String deciderName = deciderClass.getSimpleName();
                logger.error("Applying [replace-thread] strategy from [{}] decider", deciderName, exception);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        logger.error("Exhausted all recover strategies. Applying [shutdown-client] strategy.", exception);
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    /**
     * Since most exceptions are simply {@link org.apache.kafka.streams.errors.StreamsException stream exceptions}, may
     * be wise to try to identify known errors by message patterns and handle them accordingly.
     */
    public interface UncaughtExceptionDecider extends Ordered {

        /**
         * Inspect the exception received in a
         * {@link org.apache.kafka.streams.processor.internals.StreamThread stream thread} and respond either or not
         * should this {@link org.apache.kafka.streams.processor.internals.StreamThread thread} be replaced.
         *
         * @param exception the actual exception
         * @return either or not to replace this
         * {@link org.apache.kafka.streams.processor.internals.StreamThread thread}.
         */
        default boolean shouldRecover(Throwable exception) {
            return false;
        }

    }
}
