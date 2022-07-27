package io.github.leofuso.autoconfigure.actuator.kafka.streams.autopilot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * Provides access to a reliable {@link Autopilot} instance by managing its lifecycle.
 */
public class AutopilotSupport {

    private final StreamLifecycleHook hook = new StreamLifecycleHook();

    @Nullable
    private final RecoveryWindowManager windowManager;

    private final AutopilotConfiguration config;
    private final KafkaStreamsConfiguration streamConfig;

    public static AutopilotSupport automated(KafkaStreamsConfiguration streamConfig, AutopilotConfiguration config) {
        return new AutopilotSupport(true, streamConfig, config);
    }

    public static AutopilotSupport manual(KafkaStreamsConfiguration streamConfig, AutopilotConfiguration config) {
        return new AutopilotSupport(false, streamConfig, config);
    }

    private AutopilotSupport(boolean automated, KafkaStreamsConfiguration streamConfig, AutopilotConfiguration config) {
        this.windowManager = automated ? Autopilot.windowManager(config) : null;
        this.streamConfig = streamConfig;
        this.config = config;
    }

    /**
     * If an {@link Autopilot} instance is available, invoke this action onto it.
     * @param action to be invoked on to.
     * @param <T> the action return type.
     * @return the action result, if any.
     */
    public <T> Optional<T> invoke(Function<Autopilot, T> action) {
        final Optional<Autopilot> instance = hook.instance();
        return instance.map(action);
    }

    /**
     * @return a customizer to tap into the {@link KafkaStreams} lifecycle.
     */
    public StreamsBuilderFactoryBeanCustomizer lifecycleHook() {
        return factory -> factory.addListener(hook);
    }

    /**
     * @return a {@link StateListener} that delegates its actions to a {@link RecoveryWindowManager}.
     */
    public Optional<StateListener> automationHook() {
        return Optional.ofNullable(windowManager)
                       .map(RecoveryWindowManager::hookSupplier)
                       .map(Supplier::get);
    }


    private class StreamLifecycleHook implements StreamsBuilderFactoryBean.Listener {

        @Nullable
        private Autopilot autopilot;

        private Optional<Autopilot> instance() {
            return Optional.ofNullable(autopilot);
        }

        @Override
        public void streamsAdded(@Nonnull String id, @Nonnull final KafkaStreams streams) {
            autopilot = new DefaultAutopilot(streams, config, streamConfig.asProperties());
            if (windowManager != null) {
                autopilot.automate(windowManager);
            }
        }

        @Override
        public void streamsRemoved(@Nonnull String id, @Nonnull KafkaStreams streams) {
            if (autopilot != null) {
                autopilot.shutdown();
            }
        }
    }
}