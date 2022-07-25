package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

/**
 * Actuator endpoint for {@link Autopilot} manual operation.
 */
@Endpoint(id = "autopilotthread")
public class AutopilotThreadEndpoint {

    /**
     * To delegate the actions to.
     */
    private final Autopilot autopilot;

    /**
     * Creates a new {@link AutopilotThreadEndpoint} instance.
     *
     * @param autopilot to delegate the actions to.
     */
    public AutopilotThreadEndpoint(final Autopilot autopilot) {
        this.autopilot = autopilot;
    }

    /**
     * Invokes the creation of an additional StreamThread.
     * <p><strong>WARNING</strong>: This utility does not and should not respect the StreamThread limit
     * established.</p>
     */
    @WriteOperation
    public void addStreamThread() {
        autopilot.addStreamThread();
    }

    /**
     * Invokes the removal of a previously added StreamThread.
     * <p><strong>WARNING</strong>: This utility does not and should not respect the minimum amount of StreamThreads
     * that defines a health application .</p>
     */
    @DeleteOperation
    public void removeStreamThread() {
        autopilot.removeStreamThread();
    }
}
