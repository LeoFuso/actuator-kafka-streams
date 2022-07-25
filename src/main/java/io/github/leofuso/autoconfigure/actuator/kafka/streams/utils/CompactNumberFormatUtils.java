package io.github.leofuso.autoconfigure.actuator.kafka.streams.utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Used to compact big numbers.
 */
public abstract class CompactNumberFormatUtils {

    private static final NavigableMap<Long, String> suffixes = new TreeMap<>();

    static {
        suffixes.put(1_000L, "k");
        suffixes.put(1_000_000L, "M");
        suffixes.put(1_000_000_000L, "G");
        suffixes.put(1_000_000_000_000L, "T");
        suffixes.put(1_000_000_000_000_000L, "P");
        suffixes.put(1_000_000_000_000_000_000L, "E");
    }

    /**
     * @param value the number to format.
     * @return a shortened version of the provided number.
     */
    public static String format(long value) {
        if (value == Long.MIN_VALUE) {
            return format(Long.MIN_VALUE + 1);
        }
        if (value < 0) {
            return "-" + format(-value);
        }
        if (value < 1000) {
            return Long.toString(value);
        }

        Map.Entry<Long, String> e = suffixes.floorEntry(value);
        Long divideBy = e.getKey();
        String suffix = e.getValue();

        long truncated = value / (divideBy / 10);
        boolean hasDecimal = truncated < 100 && (truncated / 10d) != (truncated / (float) 10);
        return hasDecimal ? (truncated / 10d) + suffix : (truncated / 10) + suffix;
    }

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
