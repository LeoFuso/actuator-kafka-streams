package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * An {@link Autopilot} aims to avoid unnecessary horizontal auto-scale by first gathering the accumulated lag per
 * {@link TopicPartition}, and then, guided by its configuration and previous actions, triggering the creation and
 * removal of additional {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThreads}.
 *
 * <p>The {@link Autopilot} will try to maintain the lowest
 * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} count possible, limited only by the
 * <code>num.threads</code> property.</p>
 */
public interface Autopilot extends Runnable {

    /**
     * A {@link State} is used to determinate the next action an {@link Autopilot} should perform.
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
         * Creates a new {@link State} instance.
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
         * @return either or not this {@link State} can transition to the others.
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
     * The main action-loop of an {@link Autopilot}.
     * <ol>
     *     <li>Gather lag info; and update the StreamThread count;</li>
     *     <li>Check if the current {@link State} allows for additional actions;</li>
     *     <li>Decide the next state;</li>
     *     <li>Act and repeat.</li>
     * </ol>
     */
    @Override
    void run();

    /**
     * @return the next {@link State} based on  {@link #threads() thread} info.
     */
    State decideNextState();

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
     * @return the current {@link State} of the {@link Autopilot}.
     */
    State state();

    /**
     * Fetches and updates the current thread and lag info, grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} name.
     *
     * @return the current thread info, grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread} name.
     */
    Map<String, Map<TopicPartition, Long>> threads();

    /**
     * Initializes an {@link Autopilot autopilot} instance..
     */
    void initialize();

    /**
     * Stops a running {@link Autopilot autopilot}.
     *
     * @throws InterruptedException if interrupted while shutting it down.
     */
    void shutdown() throws InterruptedException;

}
