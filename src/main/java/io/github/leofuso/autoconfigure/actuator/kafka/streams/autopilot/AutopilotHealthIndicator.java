package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.Autopilot.State.BOOSTED;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.Autopilot.State.BOOSTING;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.Autopilot.State.DECREASING;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot.Autopilot.State.STAND_BY;

/**
 * Health indicator for {@link Autopilot}.
 */
public class AutopilotHealthIndicator extends AbstractHealthIndicator {

    /**
     * Helper to format huge numbers, commonly placed in situations of high partition-lag.
     */
    private static final NumberFormat numberFormat = NumberFormat.getCompactNumberInstance(
            Locale.US,
            NumberFormat.Style.SHORT
    );

    /**
     * All {@link Autopilot.State} {@link Status} representations.
     */
    public static final Map<Autopilot.State, Status> statuses =
            Map.of(
                    STAND_BY, new Status(STAND_BY.name(), STAND_BY.description()),
                    BOOSTING, new Status(BOOSTING.name(), BOOSTING.description()),
                    BOOSTED, new Status(BOOSTED.name(), BOOSTED.description()),
                    DECREASING, new Status(DECREASING.name(), DECREASING.description())
            );

    private final AutopilotSupport support;

    /**
     * Create a new AutopilotHealthIndicator instance.
     *
     * @param support to access the recorded lag.
     */
    public AutopilotHealthIndicator(AutopilotSupport support) {
        this.support = Objects.requireNonNull(support, "AutopilotSupport [support] is required.");
    }

    @Override
    protected void doHealthCheck(final Health.Builder builder) {
        try {

            final List<Map<String, Object>> threads =
                    support.invoke(Autopilot::threadInfo)
                           .orElseGet(Map::of)
                           .entrySet()
                           .stream()
                           .map(entry -> {
                               final String key = entry.getKey();
                               final Map<TopicPartition, Long> value = entry.getValue();
                               return getDetails().apply(key, value);
                           })
                           .toList();

            builder.withDetail("thread.info", threads);
            final Status status = support
                    .invoke(Autopilot::state)
                    .map(statuses::get)
                    .orElse(Status.DOWN);

            builder.status(status);

        } catch (Exception e) {
            builder.down(e);
        }
    }

    private static BiFunction<String, Map<TopicPartition, Long>, Map<String, Object>> getDetails() {

        return (thread, partitions) -> {
            final List<String> partitionDetails =
                    partitions.entrySet()
                              .stream()
                              .sorted(Map.Entry.comparingByValue())
                              .map(entry -> {
                                  final TopicPartition key = entry.getKey();
                                  final Long lag = entry.getValue();
                                  final String compactLag = numberFormat.format(lag);
                                  return String.format("[ %s\u0009] %s", compactLag, key);
                              })
                              .toList();

            return Map.ofEntries(
                    Map.entry("name", thread),
                    Map.entry("partitions", partitionDetails)
            );

        };
    }
}
