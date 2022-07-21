package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.KafkaStreams.State;
import static org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * Responsible for managing the lifecycle of all created channels between a {@link HostInfo host} and its
 * {@link RemoteStateStoreStub stub} counterpart.
 */
public interface HostManager {

    /**
     * @param key        needed in hash function used to pinpoint the correct {@link HostInfo host}.
     * @param serializer needed in hash function used to pinpoint the correct {@link HostInfo host}.
     * @param storeName  to check overall availability.
     * @param <K>        the key type.
     * @return a {@link HostInfo host} that carries a reference of given key paired with given serializer.
     */
    <K> Optional<HostInfo> findHost(K key, Serializer<K> serializer, String storeName);

    /**
     * Find a specific {@link RemoteStateStore store} by its {@link RemoteStateStore#reference() reference}.
     *
     * @param reference the reference.
     * @param <R>       the {@link RemoteStateStore storeType} type.
     * @return a {@link RemoteStateStore} associated with given {@link RemoteStateStore#reference() reference}.
     */
    <R extends RemoteStateStore> Optional<R> findStore(String reference);

    /**
     * @param host      used for lookups.
     * @param storeType used to compatibility check against the {@link RemoteStateStore store} candidates.
     * @param <R>       the {@link RemoteStateStore storeType} type.
     * @return a {@link RemoteStateStore} associated with given {@link HostInfo host}. Can be a
     * {@link RemoteStateStore local}, if the given {@link HostInfo host} points to itself, or a
     * {@link RemoteStateStore remote} one, if the given {@link HostInfo host} points to somewhere else.
     */
    <R extends RemoteStateStore> Optional<R> findStore(HostInfo host, QueryableStoreType<?> storeType);

    /**
     * Performs a clean-up by starting a shutdown process for all {@link RemoteStateStore stores} associated with this
     * {@link HostManager manager}, and removing all associated {@link HostInfo hosts} with it.
     */
    void cleanUp();

    /**
     * A {@link StateListener listener} that performs a {@link HostManager#cleanUp() clean-up} everytime a
     * {@link org.apache.kafka.streams.KafkaStreams stream} enters a {@link State#REBALANCING rebalancing} state.
     */
    class CleanUpListener implements StateListener {

        private static final Logger logger = LoggerFactory.getLogger(CleanUpListener.class);

        private final HostManager manager;

        public CleanUpListener(final HostManager manager) {
            this.manager = Objects.requireNonNull(manager, "HostManager [manager] is required.");
        }

        @Override
        public void onChange(final State newState, final State oldState) {
            if (newState == State.REBALANCING) {
                logger.info("Current partitions assignments may be revoked, starting HostManager clean-up.");
                manager.cleanUp();
            }
        }
    }

}
