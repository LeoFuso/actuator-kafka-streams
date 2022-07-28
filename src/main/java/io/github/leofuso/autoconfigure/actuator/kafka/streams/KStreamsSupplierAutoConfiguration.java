package io.github.leofuso.autoconfigure.actuator.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME;


/**
 * {@link EnableAutoConfiguration Auto-configuration} for {@link KStreamsSupplier}.
 */
@AutoConfiguration(before = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreams.class})
public class KStreamsSupplierAutoConfiguration {

    /**
     * Main bean factory for the {@link KStreamsSupplier}.
     *
     * @return a new {@link KStreamsSupplier}.
     */
    @Bean
    @DependsOn(DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    @ConditionalOnMissingBean(KStreamsSupplier.class)
    public KStreamsSupplier kStreamsSupplier(KafkaStreamsConfiguration config) {
        return new KStreamsSupplier(config);
    }

    /**
     * Used to attach a {@link org.springframework.kafka.config.StreamsBuilderFactoryBean.Listener listener} to a
     * managed {@link KafkaStreams KafkaStreams} lifecycle.
     *
     * @param supplier used to create a {@link StreamsBuilderFactoryBeanConfigurer}.
     * @return a new {@link StreamsBuilderFactoryBeanConfigurer}.
     */
    @Bean("kStreamsSupplierStreamsBuilderFactoryBeanConfigurer")
    @ConditionalOnMissingBean(name = "kStreamsSupplierStreamsBuilderFactoryBeanConfigurer")
    public StreamsBuilderFactoryBeanConfigurer configurer(KStreamsSupplier supplier) {
        return factoryBean -> factoryBean.addListener(supplier);
    }

}
