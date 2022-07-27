package io.github.leofuso.autoconfigure.actuator.kafka.streams.utils;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.ConfigKey;
import static org.apache.kafka.common.config.ConfigDef.Type;
import static org.apache.kafka.common.config.ConfigDef.parseType;

/**
 * A simply utils class to provide easy access to an {@link org.apache.kafka.common.config.Config config} structure.
 */
public abstract class ConfigUtils {

    public static <T> Optional<T> access(Properties prop, String key, ConfigDef definition) {

        final Map<String, ConfigKey> keyMap = definition.configKeys();
        final ConfigKey keyDef = keyMap.get(key);
        final Type keyType = keyDef.type;

        return Optional.ofNullable((String) prop.get(keyDef.name))
                       .map(value -> parseType(key, value, keyType))
                       .map(o -> {
                           @SuppressWarnings("unchecked")
                           final T casted = (T) o;
                           return casted;
                       })
                       .or(() -> {

                           final Map<String, Object> defaultValues = definition.defaultValues();
                           final Object defaultValue = defaultValues.get(keyDef.name);

                           @SuppressWarnings("unchecked")
                           final T parsedValue = (T) parseType(key, defaultValue, keyType);
                           return Optional.ofNullable(parsedValue);
                       });
    }
}
