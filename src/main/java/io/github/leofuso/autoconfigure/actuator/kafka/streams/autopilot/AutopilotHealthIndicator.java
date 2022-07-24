package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

/**
 * Health indicator for {@link Autopilot}.
 */
public class AutopilotHealthIndicator extends AbstractHealthIndicator {

    private static final String KEY = "Autopilot";

    /**
     * {@link Status} indicating that the Autopilot is probably applying a Boost.
     */
    public static final Status BOOST = new Status(
            "BOOST",
            "Status indicating that the Autopilot is probably applying a Boost"
    );

    /**
     * {@link Status} indicating that the Autopilot is probably applying a Nerf.
     */
    public static final Status NERF = new Status(
            "NERD",
            "Status indicating that the Autopilot is probably applying a Nerf"
    );

    private final Autopilot autopilot;

    /**
     * Create a new {@link AutopilotHealthIndicator} instance.
     *
     * @param autopilot to access the recorded lag.
     */
    public AutopilotHealthIndicator(final Autopilot autopilot) {
        this.autopilot = Objects.requireNonNull(autopilot, "Autopilot [autopilot] is required.");
    }

    @Override
    protected void doHealthCheck(final Health.Builder builder) {
        try {
            final Map<String, Object> details = new HashMap<>();
            final Map<String, Map<TopicPartition, Long>> record = autopilot.lag();
            record.forEach((thread, partitions) -> {
                final Map<String, String> partitionDetails = new HashMap<>();
                partitions.forEach(
                        (partition, lag) -> {
                            final String key = partition.toString();
                            final String value = lag.toString();
                            partitionDetails.put(key, value);
                        });
                details.put(thread, partitionDetails);
            });

            builder.withDetail(KEY, details);
            if (autopilot.shouldBoost(record)) {
                builder.status(BOOST);
                return;
            }

            if (autopilot.shouldNerf(record)) {
                builder.status(NERF);
                return;
            }
            builder.up();

        } catch (Exception e) {
            builder.down(e);
        }
    }
}
