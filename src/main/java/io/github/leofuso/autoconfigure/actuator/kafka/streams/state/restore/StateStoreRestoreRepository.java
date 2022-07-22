package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.restore;

import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.processor.StateRestoreListener;


/**
 * Serves as a carrier / accessor of all available {@link org.apache.kafka.streams.processor.StateStore StateStore}
 * restorations.
 */
public interface StateStoreRestoreRepository extends StateRestoreListener {

    /**
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states.
     */
    List<Map<String, Object>> list();

    /**
     * @param storeName {@link org.apache.kafka.streams.processor.StateStore StateStore} name to query for restoration
     *                  states.
     * @return all available {@link org.apache.kafka.streams.processor.StateStore StateStore} restoration states, if
     * any.
     */
    Map<String, Object> findByStoreName(String storeName);

}
