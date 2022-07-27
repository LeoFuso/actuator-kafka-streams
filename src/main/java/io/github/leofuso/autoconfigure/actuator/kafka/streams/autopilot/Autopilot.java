package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.CompactNumberFormatUtils;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotConfigurationProperties.Period;

/**
 * An {@link Autopilot} aims to avoid unnecessary horizontal auto-scale by first gathering the accumulated lag per
 * {@link TopicPartition}, and then, guided by its configuration and {@link State state}, triggering the creation and
 * removal of additional {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThreads}.
 *
 * <p>The {@link Autopilot} will try to maintain the lowest
 * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} count possible, limited only by the
 * {@code num.threads} property.</p>
 */
public interface Autopilot extends Runnable {

    /**
     * Coordinates the {@link Autopilot} life-cycle, and provides an easy documentation of the {@link Autopilot}
     * behavior. The {@link Autopilot} must only be in one state at a time, and all state changes must be done so in
     * coordination with all participating actors.
     * The expected state transitions with the following defined states is:
     *
     * <pre>
     *
     *                       +<--------------+
     *                       |               |
     *                       v               |
     *                 +-------------+       |
     *      +--------> | Stand By (0) |----->+
     *      |          +-----+-------+
     *      |                |
     *      |                |
     *      |                +<-------------+
     *      |                |              |
     *      |                v              |
     *      |          +-----+-------+      |
     *      |   +----> | Boosting (1)|----->+
     *      |   |      +-----+-------+
     *      |   |            |
     *      |   |            +<----------+
     *      |   |            |           |
     *      |   |            v           |
     *      |   |      +-----+-------+   |
     *      |   +<---  | Boosted (2) |---+<--+
     *      |          +-----+-------+       |
     *      |                |               |
     *      |                +<----------+   |
     *      |                |           |   |
     *      |                v           |   |
     *      |        +-------+-------+   |   |
     *      +<-------| Decreasing (3)| --+---+
     *               +-------+-------+
     *
     * </pre>
     *
     * <ol>
     *     <li><strong>Stand By</strong>: No additional StreamThreads are running, the KafkaStreams is stable.</li>
     *     <li><strong>Boosting</strong>: An additional StreamThread is being created, making the KafkaStreams unstable.</li>
     *     <li><strong>Boosted</strong>: One or more additional StreamThreads are running, the KafkaStream is stable.</li>
     *     <li><strong>Decreasing</strong>: One or more additional StreamThreads are being removed, making the KafkaStreams unstable.</li>
     * </ol>
     *
     */
    enum State {

        STAND_BY(
                "Autopilot is on stand-by. No actions to perform.",
                0, 1
        ),
        BOOSTING(
                "Autopilot is boosting the StreamThread count, and will remain until the StreamThread count hits its target.",
                1, 2
        ),
        BOOSTED(
                "Autopilot has finished boosting the StreamThread count.",
                1, 2, 3
        ),
        DECREASING(
                "Autopilot has lowered the StreamThread count target, and now is triggering the removal of additional StreamThreads.",
                0, 2, 3
        );

        /**
         * {@link State} description.
         */
        private final String description;

        /**
         * The {@link State states} in which this {@link State} can transition to.
         */
        private final Set<Integer> transitions = new HashSet<>();

        /**
         * Creates a new State instance.
         *
         * @param description the description.
         * @param transitions all the transitions.
         */
        State(String description, Integer... transitions) {
            this.description = description;
            this.transitions.addAll(Arrays.asList(transitions));
        }

        /**
         * @param others to transit to.
         * @return either or not this State can transition to the others.
         */
        public boolean isValidTransition(final State... others) {
            for (final State other : others) {
                final int ordinal = other.ordinal();
                final boolean contains = transitions.contains(ordinal);
                if (contains) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @return the description.
         */
        public String getDescription() {
            return description;
        }
    }

    /**
     * Static factory for {@link RecoveryWindow} creations.
     * @param properties used to build a new {@link RecoveryWindow} instance.
     * @return a newly built {@link RecoveryWindow} instance.
     */
    static RecoveryWindow recoveryWindow(AutopilotConfigurationProperties properties) {
        final Period period = properties.getPeriod();
        final Duration window = period.getRecoveryWindow();
        return new RecoveryWindow(window);
    }

    /**
     * The main action-loop of an Autopilot.
     * <ol>
     *     <li>Gather lag info; and update the StreamThread count;</li>
     *     <li>Check if the current {@link State} is stable;
     *      <ol>
     *          <li> if it is, do nothing; keep state;</li>
     *          <li> if it is not, decide next state; update it;</li>
     *      </ol>
     *     </li>
     *     <li>Act accordingly to decided state, and repeat.</li>
     * </ol>
     */
    @Override
    void run();

    /**
     * Invoke the creation of an additional
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread}.
     */
    void addStreamThread();

    /**
     * Invoke the removal of a {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread}.
     */
    void removeStreamThread();

    /**
     * @return the current {@link State} of the Autopilot.
     */
    State state();

    /**
     * @return the {@link RecoveryWindow} associated with this Autopilot.
     */
    RecoveryWindow recoveryWindow();

    /**
     * Fetches and updates the current thread and lag info, grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} name.
     *
     * @return the current thread info, grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} name.
     */
    Map<String, Map<TopicPartition, Long>> threads();

    /**
     * Initializes this instance.
     */
    void initialize();

    /**
     * Stops any running actions and closes this instance.
     */
    void shutdown();

    /**
     * Will maintain a {@link Duration window} which a KafkaStreams App can recover from a high partition-lag value.
     * This {@link Duration window} only opens after a successful StreamThread count update.
     */
    class RecoveryWindow implements KafkaStreams.StateListener {

        /**
         * {@link KafkaStreams.State State} stable enough to accept {@link Autopilot} actions.
         */
        public static final KafkaStreams.State STABLE_STATE = KafkaStreams.State.RUNNING;

        private static final Logger logger = LoggerFactory.getLogger(RecoveryWindow.class);

        /**
         * Used to keep time between two {@link RecoveryWindow#STABLE_STATE stable} states.
         */
        private final Clock clock = Clock.systemUTC();

        /**
         * The point-in-time marking the last recorded {@link RecoveryWindow#STABLE_STATE stable} state.
         */
        private final Instant timestamp = Instant.EPOCH;

        /**
         * A {@link Duration period} of stability to maintain between each transition.
         */
        private final Duration window;

        /**
         * Constructs a RecoveryWindow instance, starting its internal clock.
         *
         * @param window A {@link Duration period} of stability to maintain between each transition.
         */
        RecoveryWindow(Duration window) {
            this.window = Objects.requireNonNull(window, "Duration [window] is required.");
        }

        @Override
        public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
            if (newState == STABLE_STATE) {
                final Instant timestamp = Instant.now(clock);
                this.timestamp.with(timestamp);
                logger.debug("KafkaStreams has reached a stable state, timeout clock is ticking.");
            }
        }

        /**
         * @return either or not the recovery window has closed.
         */
        boolean hasClosed() {

            final Instant checkpoint = Instant.now(clock);
            final Duration elapsed = Duration.between(timestamp, checkpoint);

            final boolean isClosed = elapsed.compareTo(window) >= 0;
            if (!isClosed) {
                final Duration remaining = window.minus(elapsed);
                final String prettyWindow = CompactNumberFormatUtils.format(window);
                final String prettyRemaining = CompactNumberFormatUtils.format(remaining);
                logger.info(
                        "Autopilot [NOOP]. A recovery window of {} is in place, and will remain open for another {}.",
                        prettyWindow,
                        prettyRemaining
                );
                return false;
            }
            return true;
        }
    }
}
