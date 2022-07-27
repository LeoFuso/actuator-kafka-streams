package io.github.leofuso.autoconfigure.actuator.kafka.streams.state;

import org.apache.kafka.streams.processor.StateRestoreListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.CompositeStateListener;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.DefaultCompositeStateListener;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore.CompositeStateRestoreListener;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore.DefaultCompositeStateRestoreListener;

import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * {@link EnableAutoConfiguration Auto-configuration} that groups all {@link StateListener StateListeners} and
 * {@link StateRestoreListener StateRestoreListeners} into its Composite counterparts.
 */
@AutoConfiguration(after = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
public class CompositeStateAutoConfiguration {

    @Bean
    @ConditionalOnBean(StateListener.class)
    @ConditionalOnMissingBean(CompositeStateListener.class)
    public CompositeStateListener compositeStateListener(ObjectProvider<StateListener> provider) {
        return new DefaultCompositeStateListener(provider.orderedStream());
    }

    @Bean
    @ConditionalOnBean(StateRestoreListener.class)
    @ConditionalOnMissingBean(CompositeStateRestoreListener.class)
    public CompositeStateRestoreListener compositeStateRestoreListener(ObjectProvider<StateRestoreListener> provider) {
        return new DefaultCompositeStateRestoreListener(provider.orderedStream());
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamsBuilderFactoryBeanCustomizer stateListenerCustomizer(ObjectProvider<CompositeStateListener> state) {
        final CompositeStateListener listener = state.getIfAvailable();
        if (listener != null) {
            return fb -> fb.setStateListener(listener);
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamsBuilderFactoryBeanCustomizer stateRestoreListenerCustomizer(ObjectProvider<CompositeStateRestoreListener> state) {
        final CompositeStateRestoreListener listener = state.getIfAvailable();
        if (listener != null) {
            return fb -> fb.setStateRestoreListener(listener);
        }
        return null;
    }

}
