package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * An {@link Autopilot} aims to avoid unnecessary horizontal auto-scale by accessing the recorded accumulated
 * lag per {@link org.apache.kafka.streams.TaskMetadata task} and automatically invoking the creation and removal of
 * additional {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThreads}.
 */
public interface Autopilot extends Runnable {

    /**
     * Invoke the creation of an additional
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread}.
     */
    void addStreamThread();

    /**
     * Invoke the removal of a previously created additional
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThreads}.
     */
    void removeStreamThread();

    /**
     * @return the current lag, grouped by
     * {@link org.apache.kafka.streams.processor.internals.StreamThread StreamThread}.
     */
    Map<String, Map<TopicPartition, Long>> lag();

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
