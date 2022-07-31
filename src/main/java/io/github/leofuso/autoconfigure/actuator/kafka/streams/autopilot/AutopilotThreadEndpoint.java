package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

/**
 * Actuator endpoint for {@link Autopilot} manual operation.
 */
@Endpoint(id = "autopilotthread")
public class AutopilotThreadEndpoint {

    /**
     * To delegate actions to.
     */
    private final AutopilotSupport support;

    /**
     * Creates a new AutopilotThreadEndpoint instance.
     *
     * @param support to delegate the actions to.
     */
    public AutopilotThreadEndpoint(AutopilotSupport support) {
        this.support = Objects.requireNonNull(support, "Autopilot [support] is required.");
    }

    /**
     * Invokes the creation of an additional StreamThread.
     * <p><strong>WARNING</strong>: This utility does not and should not respect the StreamThread limit
     * established.</p>
     */
    @WriteOperation
    public void addStreamThread() {
        final Duration timeout = Duration.ofMinutes(1);
        support.invoke(autopilot -> autopilot.addStreamThread(timeout))
               .map(future -> future.thenApply(Optional::ofNullable))
               .flatMap(CompletableFuture::join)
               .orElseThrow();
    }

    /**
     * Invokes the removal of a previously added StreamThread.
     * <p><strong>WARNING</strong>: This utility does not and should not respect the minimum amount of StreamThreads
     * that defines a health application .</p>
     */
    @DeleteOperation
    public void removeStreamThread() {
        final Duration timeout = Duration.ofMinutes(1);
        support.invoke(autopilot -> autopilot.removeStreamThread(timeout))
               .map(future -> future.thenApply(Optional::ofNullable))
               .flatMap(CompletableFuture::join)
               .orElseThrow();
    }
}
