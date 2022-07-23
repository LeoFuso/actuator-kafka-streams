package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "management.health.kstreams")
public class KafkaStreamHealthIndicatorProperties {

    /**
     * A required minimum number of running {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads}.
     * Ignored if {@link KafkaStreamHealthIndicatorProperties#allowThreadLoss thread loss allowance} is false.
     */
    private int minimumNumberOfLiveStreamThreads = 1;

    /**
     * Either or not to allow for any of the initially requested
     * {@link org.apache.kafka.streams.processor.internals.StreamThread stream threads} to shutdown with errors.
     */
    private boolean allowThreadLoss = true;

    public boolean isAllowThreadLoss() {
        return allowThreadLoss;
    }

    public void setAllowThreadLoss(final boolean allowThreadLoss) {
        this.allowThreadLoss = allowThreadLoss;
    }

    public int getMinimumNumberOfLiveStreamThreads() {
        return minimumNumberOfLiveStreamThreads;
    }

    public void setMinimumNumberOfLiveStreamThreads(final int minimumNumberOfLiveStreamThreads) {
        this.minimumNumberOfLiveStreamThreads = minimumNumberOfLiveStreamThreads;
    }
}
