package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.CompactNumberFormatUtils;

/**
 * Health indicator for {@link Autopilot}.
 */
public class AutopilotHealthIndicator extends AbstractHealthIndicator {

    /**
     * {@link Status} indicating that the Autopilot is probably applying a Boost.
     */
    public static final Status BOOST = new Status(
            "BOOST",
            "Autopilot is trying to increase StreamThread count, by applying a Boost."
    );

    /**
     * {@link Status} indicating that the Autopilot is probably applying a Nerf.
     */
    public static final Status NERF = new Status(
            "NERF",
            "Autopilot is trying to reduce StreamThread count, by applying a Nerf."
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

            final ArrayList<Map<String, Object>> details = new ArrayList<>();
            final Map<String, Map<TopicPartition, Long>> record = autopilot.lag();
            record.forEach(addDetails(details));
            builder.withDetail("threads", details);

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

    private static BiConsumer<String, Map<TopicPartition, Long>> addDetails(final ArrayList<Map<String, Object>> details) {
        return (thread, partitions) -> {

            final List<String> topicPartitions =
                    partitions.entrySet()
                              .stream()
                              .sorted(Map.Entry.comparingByValue())
                              .map(entry -> {
                                  final TopicPartition key = entry.getKey();
                                  final Long lag = entry.getValue();
                                  final String compactLag = CompactNumberFormatUtils.format(lag);
                                  return String.format("[ %s\t] %s", compactLag, key);
                              })
                              .collect(Collectors.toList());

            final Map<String, Object> threadDetails = Map.of(
                    "partitionLag", topicPartitions,
                    "threadName", thread
            );

            details.add(threadDetails);
        };
    }
}
