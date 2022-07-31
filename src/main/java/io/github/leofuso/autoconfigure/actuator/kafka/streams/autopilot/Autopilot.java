package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.AutopilotConfiguration.Period;

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
     * {@code
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
     * }
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
        public String description() {
            return description;
        }
    }

    /**
     * Static factory for {@link RecoveryWindowManager}.
     * @param configuration used to configure the new {@link RecoveryWindowManager} instance.
     * @return a newly built {@link RecoveryWindowManager} instance.
     */
    static RecoveryWindowManager windowManager(AutopilotConfiguration configuration) {
        final Period period = configuration.getPeriod();
        final Duration window = period.getRecoveryWindow();
        return new RecoveryWindowManager(window);
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
     *
     * @param timeout to finish the operation.
     * @return a {@link CompletableFuture} containing the created thread name, or nothing, exceptionally.
     */
    CompletableFuture<String> addStreamThread(Duration timeout);

    /**
     * Invoke the removal of a {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread}.
     *
     * @param timeout to finish the operation.
     * @return a {@link CompletableFuture} containing the removed thread name, or nothing, exceptionally.
     */
    CompletableFuture<String> removeStreamThread(Duration timeout);

    /**
     * @return the current {@link State state} of the Autopilot.
     */
    State state();

    /**
     * Fetches the most up-to-date thread info, by updating it first. All thread info is grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} name.
     *
     * @return the most up-to-date thread info, grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} name.
     */
    Map<String, Map<TopicPartition, Long>> threadInfo();

    /**
     * Automates this instance by initializing its {@link #run() run-loop} with a
     * {@link RecoveryWindowManager window manager} capable of keeping KafkaStreams events.
     *
     * @param windowManager capable to react to KafkaStreams events.
     */
    void automate(@Nonnull RecoveryWindowManager windowManager);

    /**
     * Stops any running actions by closing its {@link java.util.concurrent.Executor executor service}.
     */
    void shutdown();

}
