package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import org.apache.kafka.streams.processor.StateRestoreListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * {@link EnableAutoConfiguration Auto-configuration} that groups all {@link StateListener StateListeners} and
 * {@link StateRestoreListener StateRestoreListeners} into its Composite counterparts.
 */
@AutoConfiguration(before = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
public class CompositeStateAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(CompositeStateListener.class)
    public CompositeStateListener stateListener(ObjectProvider<StateListener> provider) {
        return new DefaultCompositeStateListener(provider);
    }

    @Bean
    @ConditionalOnMissingBean(CompositeStateRestoreListener.class)
    public CompositeStateRestoreListener stateRestoreListener(ObjectProvider<StateRestoreListener> provider) {
        return new DefaultCompositeStateRestoreListener(provider);
    }

}
