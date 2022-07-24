package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "management.health.autopilot")
public class AutopilotConfigurationProperties {

    /**
     * An upper bound of allowed StreamThreads running simultaneously.
     */
    @NotNull
    @PositiveOrZero
    private Integer streamThreadLimit = 5;

    /**
     * To trigger the creation or removal of new StreamThreads.
     */
    @NotNull
    @Positive
    private Long lagThreshold = 20_000L;

    /**
     * A period between Autopilot runs.
     */
    @NotNull
    private Duration period = Duration.ofMinutes(1);

    /**
     * A general timeout required for Autopilot actions.
     */
    @NotNull
    private Duration timeout = Duration.ofMillis(600);

    public Integer getStreamThreadLimit() {
        return streamThreadLimit;
    }

    public Long getLagThreshold() {
        return lagThreshold;
    }

    public Duration getPeriod() {
        return period;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setStreamThreadLimit(final Integer streamThreadLimit) {
        this.streamThreadLimit = streamThreadLimit;
    }

    public void setLagThreshold(final Long lagThreshold) {
        this.lagThreshold = lagThreshold;
    }

    public void setPeriod(final Duration period) {
        this.period = period;
    }

    public void setTimeout(final Duration timeout) {
        this.timeout = timeout;
    }

}
