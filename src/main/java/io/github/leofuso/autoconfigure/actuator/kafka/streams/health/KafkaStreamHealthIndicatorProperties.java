package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for {@link KafkaStreamsHealthIndicator}.
 */
@ConfigurationProperties(prefix = "management.health.kstreams")
public class KafkaStreamHealthIndicatorProperties {

    /**
     * A required minimum number of running StreamThreads. Ignored if allowThreadLoss is false.
     */
    private int minimumNumberOfLiveStreamThreads = 1;

    /**
     * Allows any of the initially requested StreamThreads to shutdown with errors.
     */
    private boolean allowThreadLoss = true;

    /**
     * @return either or not a thread loss is permitted.
     */
    public boolean isAllowThreadLoss() {
        return allowThreadLoss;
    }

    /**
     * Configure either or not to allow for thread loss.
     * @param allowThreadLoss the configuration.
     */
    public void setAllowThreadLoss(final boolean allowThreadLoss) {
        this.allowThreadLoss = allowThreadLoss;
    }

    /**
     * @return the minimum of live threads required.
     */
    public int getMinimumNumberOfLiveStreamThreads() {
        return minimumNumberOfLiveStreamThreads;
    }

    /**
     * Configure the minimum count requirement of live threads.
     * @param minimumNumberOfLiveStreamThreads the configuration.
     */
    public void setMinimumNumberOfLiveStreamThreads(final int minimumNumberOfLiveStreamThreads) {
        this.minimumNumberOfLiveStreamThreads = minimumNumberOfLiveStreamThreads;
    }
}
