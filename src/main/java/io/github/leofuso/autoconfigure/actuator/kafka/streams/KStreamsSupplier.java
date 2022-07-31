package io.github.leofuso.autoconfigure.actuator.kafka.streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * A naive {@link Supplier} that holds a reference of a managed {@link KafkaStreams}.
 */
public class KStreamsSupplier implements Supplier<Optional<KafkaStreams>>, StreamsBuilderFactoryBean.Listener {

    /**
     * The KafkaStreams properties class.
     */
    private final KafkaStreamsConfiguration configuration;

    /**
     * Holder of a {@code nullable} reference of {@link KafkaStreams}. Specially useful for avoiding cyclic references.
     */
    @Nullable
    private KafkaStreams kafkaStreams;

    /**
     * Constructs a new KStreamsSupplier.
     *
     * @param config that can be passed around when needed.
     */
    KStreamsSupplier(KafkaStreamsConfiguration config) {
        this.configuration = Objects.requireNonNull(config, "KafkaStreamsConfiguration [config] is missing.");
    }


    @Override
    public Optional<KafkaStreams> get() {
        return Optional.ofNullable(kafkaStreams);
    }

    /**
     * @return a {@link KafkaStreams} instance or throws a {@link StreamsNotStartedException}.
     */
    public KafkaStreams getOrThrows() {
        final Optional<KafkaStreams> supply = get();
        if (supply.isEmpty()) {
            throw new StreamsNotStartedException("KafkaStreams must be available.");
        }
        return supply.get();
    }

    /**
     * Return the configuration map as a {@link Properties}.
     *
     * @return the properties.
     */
    public Properties configAsProperties() {
        return configuration.asProperties();
    }

    /**
     * Return the configuration map as a {@link KafkaStreamsConfiguration}.
     *
     * @return the properties.
     */
    public KafkaStreamsConfiguration config() {
        return configuration;
    }

    @Override
    public void streamsAdded(@Nonnull String id, @Nonnull KafkaStreams streams) {
        this.kafkaStreams = streams;
    }

    @Override
    public void streamsRemoved(@Nonnull String id, @Nonnull KafkaStreams streams) {
        this.kafkaStreams = null;
    }
}
