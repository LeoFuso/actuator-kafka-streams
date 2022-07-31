package io.github.leofuso.autoconfigure.actuator.kafka.streams.utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Used to compact the default {@link Duration#toString()} return.
 */
public abstract class CompactDurationFormat {

    /**
     * @param duration the {@link Duration} to pretty print.
     * @return a shortened version of the provided {@link Duration}.
     */
    public static String format(Duration duration) {
        return duration.truncatedTo(ChronoUnit.MILLIS)
                       .toString()
                       .substring(2)
                       .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                       .toLowerCase();
    }

}
