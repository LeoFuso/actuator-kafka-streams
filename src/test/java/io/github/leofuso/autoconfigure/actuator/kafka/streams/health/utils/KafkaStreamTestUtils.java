package io.github.leofuso.autoconfigure.actuator.kafka.streams.health.utils;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.google.common.util.concurrent.MoreExecutors;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class KafkaStreamTestUtils {

    @SafeVarargs
    public static void produce(EmbeddedKafkaBroker embeddedKafka, ProducerRecord<String, String>... records) {

        Map<String, Object> prodProps = KafkaTestUtils.producerProps(embeddedKafka);
        prodProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(prodProps);

        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
        CountDownLatch latch = new CountDownLatch(records.length);
        for (ProducerRecord<String, String> record : records) {
            ListenableFuture<SendResult<String, String>> future = template.send(record);
            future.addCallback(
                    new ListenableFutureCallback<>() {
                        @Override
                        public void onFailure(@Nonnull Throwable e) {throw new RuntimeException(e);}

                        @Override
                        public void onSuccess(SendResult<String, String> result) {
                            latch.countDown();
                        }
                    });
        }
        try {
            final boolean await = latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            pf.destroy();
        }
    }

    public static void expect(HealthIndicator indicator, Status expected) throws Throwable {
        expect(indicator, expected, Duration.ofMillis(100));
    }

    public static void expect(HealthIndicator indicator, Status expected, Duration initDelay) throws Throwable {

        final int tries = 3;
        final Duration betweenAttempts = Duration.ofSeconds(5);

        final long delay = betweenAttempts.toMillis();
        final long timeout = betweenAttempts.toMillis() * (tries + 1);

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        final Runnable doHealthCheck = () -> {
            final Health health = indicator.health();
            final Status actual = health.getStatus();
            if (actual != expected) {
                return;
            }
            assertThat(actual).isSameAs(expected);
            /* We need to end the execution */
            @SuppressWarnings({"UnstableApiUsage", "unused"})
            final boolean termination = MoreExecutors.shutdownAndAwaitTermination(executor, 0, TimeUnit.MILLISECONDS);
        };

        try {
            final long initialDelay = initDelay.toMillis();
            executor.scheduleAtFixedRate(doHealthCheck, initialDelay, delay, TimeUnit.MILLISECONDS)
                    .get(initialDelay + timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | CancellationException ignored) { /* assertTrue(true) */ }
    }

}
