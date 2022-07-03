package io.github.leofuso.autoconfigure.actuator.kafka.streams.health;

import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@ConditionalOnClass(value = {KafkaStreamsDefaultConfiguration.class})
@ConditionalOnBean(value = {StreamsBuilderFactoryBean.class})
@ConditionalOnEnabledHealthIndicator("kafkaStreams")
@AutoConfigureAfter({KafkaStreamsDefaultConfiguration.class})
public class KafkaStreamsHealthIndicatorAutoConfiguration {



}
