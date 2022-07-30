package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.CompactNumberFormatUtils;

/**
 * Used as a State carrier of all restorations performed during the lifecycle of the Stream application.
 */
public class ConcurrentStateStoreRestoreCarrier implements StateStoreRestoreRepository {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentStateStoreRestoreCarrier.class);

    private final ConcurrentHashMap<String, Map<String, Object>> restorations;
    private final Clock clock;

    public ConcurrentStateStoreRestoreCarrier(final Clock clock) {
        this.restorations = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    private static String buildRestorationKey(final TopicPartition topicPartition) {
        return topicPartition.toString();
    }

    @Override
    public List<Map<String, Object>> list() {
        return restorations.entrySet().stream()
                     .map(entry -> Map.of(entry.getKey(), (Object) entry.getValue()))
                     .collect(Collectors.toList());
    }

    @Override
    public Map<String, Object> findByStoreName(final String storeName) {
        if (storeName == null || storeName.isBlank()) {
            return null;
        }

        final String keyFound = restorations.searchKeys(100, key -> {
            final String cleanStoreName = storeName.toLowerCase().strip();
            final boolean contains = key.contains(cleanStoreName);
            if (contains) {
                return key;
            }
            return null;
        });
        if (keyFound != null) {
            return restorations.get(keyFound);
        }
        return null;
    }

    @Override
    public void onRestoreStart(final TopicPartition topicPartition,
                               final String storeName,
                               final long startingOffset,
                               final long endingOffset) {

        final Instant checkpoint = Instant.now(clock);
        final long checkpointMilli = checkpoint.toEpochMilli();

        accessStateRestore(topicPartition, storeName, state -> {
            final Map<String, Long> offset = Map.of(
                    "starting.offset", startingOffset,
                    "ending.offset", endingOffset
            );
            state.put("start.checkpoint", checkpointMilli);
            state.put("offset", offset);
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onBatchRestored(final TopicPartition topicPartition,
                                final String storeName,
                                final long batchEndOffset,
                                final long numRestored) {

        final Instant batchCheckpoint = Instant.now(clock);
        final long checkpointMilli = batchCheckpoint.toEpochMilli();

        accessStateRestore(topicPartition, storeName, state -> {

            final String batchesKey = "batches";
            final List<Map<String, Object>> batches =
                    (List<Map<String, Object>>) state.getOrDefault(batchesKey, new ArrayList<>());

            final Map<String, Object> batch = Map.of(
                    "checkpoint", checkpointMilli,
                    "batch.endOffset", batchEndOffset,
                    "num.restored", numRestored
            );

            batches.add(batch);
            state.put(batchesKey, batches);
        });
    }

    @Override
    public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
        final Instant checkpointEnd = Instant.now(clock);
        final long checkpointMilli = checkpointEnd.toEpochMilli();

        accessStateRestore(topicPartition, storeName, state -> {
            state.put("total.restored", totalRestored);
            state.put("end.checkpoint", checkpointMilli);

            final Instant startCheckpoint = Instant
                    .ofEpochMilli((long) state.getOrDefault("start.checkpoint", checkpointMilli));

            logger.info(
                    "Restoration [ {}, {} ] took {}. Restored entries [ {} ].",
                    storeName,
                    topicPartition,
                    CompactNumberFormatUtils.format(Duration.between(startCheckpoint, checkpointEnd)),
                    totalRestored
            );
        });

    }

    @SuppressWarnings("unchecked")
    private void accessStateRestore(
            final TopicPartition topicPartition,
            final String store,
            final Consumer<Map<String, Object>> action
    ) {
        final String key = buildRestorationKey(topicPartition);
        final Map<String, Object> restore = restorations.getOrDefault(store, new HashMap<>());
        final Map<String, Object> stateRestore = (Map<String, Object>) restore.getOrDefault(key, new HashMap<>());
        action.accept(stateRestore);
        restore.put(key, stateRestore);
        restorations.put(store, restore);
    }

}
