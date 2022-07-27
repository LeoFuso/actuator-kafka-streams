package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

import java.time.Duration;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for Autopilot.
 */
@Validated
@ConfigurationProperties(prefix = "management.health.autopilot")
public class AutopilotConfiguration {

    /**
     * Excludes topics from the lag calculation matching the specified pattern.
     */
    @NotNull
    private Pattern exclusionPattern = Pattern.compile(
            ".*(-changelog|subscription-registration-topic|-subscription-response-topic)$",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * An upper bound of all StreamThreads the Autopilot can coordinate.
     */
    @NotNull
    @PositiveOrZero
    private Integer streamThreadLimit = 0;

    /**
     * To trigger the addition or removal of StreamThreads.
     */
    @NotNull
    @PositiveOrZero
    private Long lagThreshold = 20_000L;

    /**
     * Inner-properties exclusive to period related Autopilot behavior.
     */
    @Valid
    @NotNull
    private Period period = new Period();


    /**
     * Period configuration properties for Autopilot.
     */
    public static class Period {

        /**
         * A waiting period before the first Autopilot run.
         */
        @NotNull
        private Duration initialDelay = Duration.ofSeconds(150);

        /**
         * A period between Autopilot runs.
         */
        @NotNull
        private Duration betweenRuns = Duration.ofMinutes(5);

        /**
         * The period Autopilot will wait for the StreamThreads to stabilize.
         */
        @NotNull
        private Duration recoveryWindow = Duration.ofMinutes(10);

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(final Duration initialDelay) {
            this.initialDelay = initialDelay;
        }

        public Duration getBetweenRuns() {
            return betweenRuns;
        }

        public void setBetweenRuns(final Duration betweenRuns) {
            this.betweenRuns = betweenRuns;
        }

        public Duration getRecoveryWindow() {
            return recoveryWindow;
        }

        public void setRecoveryWindow(final Duration recoveryWindow) {
            this.recoveryWindow = recoveryWindow;
        }
    }


    public Pattern getExclusionPattern() {
        return exclusionPattern;
    }

    public Integer getStreamThreadLimit() {
        return streamThreadLimit;
    }

    public Long getLagThreshold() {
        return lagThreshold;
    }

    public Period getPeriod() {
        return period;
    }

    public void setExclusionPattern(final Pattern exclusionPattern) {
        this.exclusionPattern = exclusionPattern;
    }

    public void setStreamThreadLimit(final Integer streamThreadLimit) {
        this.streamThreadLimit = streamThreadLimit;
    }

    public void setLagThreshold(final Long lagThreshold) {
        this.lagThreshold = lagThreshold;
    }

    public void setPeriod(final Period period) {
        this.period = period;
    }
}
