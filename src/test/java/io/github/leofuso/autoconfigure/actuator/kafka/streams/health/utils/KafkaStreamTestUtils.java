package io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;

public abstract class KafkaStreamTestUtils {

    @SafeVarargs
    public static void receive(ProducerRecord<String, String>... records) throws Exception {

        final String[] topics = Stream
                .of(records)
                .map(ProducerRecord::topic)
                .distinct()
                .toArray(String[]::new);

        EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-id0", "false", embeddedKafka);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);

        try (Consumer<String, String> consumer = cf.createConsumer()) {
            KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);

            CountDownLatch latch = new CountDownLatch(records.length);
            for (ProducerRecord<String, String> record : records) {

                ListenableFuture<SendResult<String, String>> future = template.send(record);
                future.addCallback(
                        new ListenableFutureCallback<>() {

                            @Override
                            @SuppressWarnings("NullableProblems")
                            public void onFailure(Throwable ignored) {}

                            @Override
                            public void onSuccess(SendResult<String, String> result) {
                                latch.countDown();
                            }
                        });
            }

            @SuppressWarnings("unused")
            final boolean await = latch.await(2, TimeUnit.SECONDS);
            embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
            TimeUnit.SECONDS.sleep(2);
            KafkaTestUtils.getRecords(consumer, 1000);
        } finally {
            pf.destroy();
        }
    }
}
