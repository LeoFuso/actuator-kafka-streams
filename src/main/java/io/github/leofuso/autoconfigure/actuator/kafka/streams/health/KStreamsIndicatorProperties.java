package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for KafkaStreamsHealthIndicator.
 */
@ConfigurationProperties(prefix = "management.health.kstreams")
public class KStreamsIndicatorProperties {

    /**
     * A required minimum number of running StreamThreads. Ignored if "allow-thread-loss" is false.
     */
    private int minimumNumberOfLiveStreamThreads = 1;

    /**
     * Whether to allow any of the initially requested StreamThreads to shutdown with errors.
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
