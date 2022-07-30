package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.CompactNumberFormatUtils;

import static org.apache.kafka.streams.KafkaStreams.State;
import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * Will keep a count-down, in form of a time window, which can be used by the KafkaStreams to recover from a high
 * partition-lag value. This {@link Duration window} only opens after the KafkaStreams has reached a known stable
 * state.
 */
class RecoveryWindowManager {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryWindowManager.class);

    /**
     * Used to apply a recovery window count down.
     */
    private final Clock clock = Clock.systemUTC();

    /**
     * The point-in-time marking the last recorded window start.
     */
    private Instant timestamp = Instant.MIN;

    /**
     * A {@link Duration period} of stability to maintain between each transition.
     */
    private final Duration window;

    private final StateListener listener = new RecoverHook();

    /**
     * Constructs a RecoveryWindow instance, starting its internal clock.
     *
     * @param window A {@link Duration period} of stability to maintain between each transition.
     */
    RecoveryWindowManager(Duration window) {
        this.window = Objects.requireNonNull(window, "Duration [window] is required.");
    }

    public Supplier<StateListener> hookSupplier() {
        return () -> (StateListener) listener;
    }

    /**
     * @return either or not the recovery window has closed.
     */
    @SuppressWarnings("unused")
    boolean isClosed() {
        return !isOpen();
    }

    /**
     * @return either or not the recovery window is open.
     */
    boolean isOpen() {

        final Instant checkpoint = Instant.now(clock);
        final Duration elapsed = Duration.between(timestamp, checkpoint);

        final boolean isOpen = elapsed.compareTo(window) < 0;
        if (isOpen && logger.isDebugEnabled()) {

            final Duration remaining = window.minus(elapsed);
            if(remaining.isNegative()) {
                logger.debug("A recovery window is in place, and will remain open for an unknown amount of time.");
                return true;
            }

            final String prettyWindow = CompactNumberFormatUtils.format(window);
            final String prettyRemaining = CompactNumberFormatUtils.format(remaining);
            logger.debug(
                    "A recovery window of {} is in place, and will remain open for another {}.",
                    prettyWindow,
                    prettyRemaining
            );
            return true;
        }
        return false;
    }

    private class RecoverHook implements StateListener {

        /**
         * A new {@link State#RUNNING Running} state should trigger a new recovery window countdown.
         */
        public static final State RECOVER_TRIGGER = State.RUNNING;

        @Override
        public void onChange(final State newState, final State oldState) {
            if (newState == RECOVER_TRIGGER) {
                timestamp = Instant.now(clock);
                logger.debug("KafkaStreams has reached a known stable state, the recovery window count-down has begun.");
            }
            timestamp = Instant.MIN;
        }
    }
}