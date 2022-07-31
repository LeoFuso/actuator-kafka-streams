package io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.core.ResolvableType;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.additional.serdes.exceptions.AdditionalConfigException;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.Type;
import static org.apache.kafka.common.config.ConfigDef.Validator;
import static org.apache.kafka.common.config.ConfigDef.parseType;

/**
 * Configuration for any additional {@link org.apache.kafka.common.serialization.Serde Serdes} instances.
 * <p>
 * You must prefix all needed properties using {@link #additionalSerdesPropertiesPrefix(String)} to avoid property
 * conflict.
 */
public class AdditionalSerdesConfig extends AbstractConfig {

    /**
     * Root for all properties associated with {@link AdditionalSerdesConfig}.
     */
    public static final String ADDITIONAL_SERDES_CONFIG = "additional.serdes";

    /**
     * <code>additional.serdes</code> doc.
     */
    public static final String ADDITIONAL_SERDES_DOC =
            "A comma delimited list containing the fully qualified name of all additional serdes in which its " +
            "lifecycle can be managed by the framework.";

    /**
     * Root for all additional properties associated with {@link AdditionalSerdesConfig}.
     */
    public static final String ADDITIONAL_SERDES_PROPERTIES_PREFIX = ADDITIONAL_SERDES_CONFIG + "properties";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(
                        ADDITIONAL_SERDES_CONFIG,
                        Type.LIST,
                        Collections.emptyList(),
                        ValidSerdesClassList.getInstance(),
                        Importance.LOW,
                        ADDITIONAL_SERDES_DOC
                );

    }

    /**
     * Create a new {@link AdditionalSerdesConfig} using the given properties.
     *
     * @param properties properties that specify additional {@link Serde serdes}.
     */
    public AdditionalSerdesConfig(final Map<?, ?> properties) {
        super(CONFIG, properties);
    }

    /**
     * Prefix a property with {@link #ADDITIONAL_SERDES_PROPERTIES_PREFIX}. This is used to isolate
     * {@link AdditionalSerdesConfig additional serdes configs} from other client configs.
     *
     * @param property the {@link Serde} property to be masked
     * @return ADDITIONAL_SERDES_PROPERTIES_PREFIX + {@code property}
     */
    public static String additionalSerdesPropertiesPrefix(final String property) {
        return ADDITIONAL_SERDES_PROPERTIES_PREFIX + property;
    }

    /**
     * Returns the underlying {@link Serde serde} type.
     *
     * @param serde That needs to have its underlying type extracted.
     * @param <T>   the resulting underlying type.
     * @return the underlying {@link Serde serde} type.
     */
    public static <T> Class<T> underlyingSerdeType(Serde<T> serde) {
        final int serdeTypeIndex = 0;
        final ResolvableType resolvableType = ResolvableType.forInstance(serde)
                                                            .as(Serde.class);

        @SuppressWarnings("unchecked")
        final Class<T> serdeType = (Class<T>) resolvableType.resolveGeneric(serdeTypeIndex);
        return serdeType;
    }

    /**
     * Returns the underlying {@link Serde serde} type.
     *
     * @param serde That needs to have its underlying type extracted.
     * @param <T>   the resulting underlying type.
     * @param <S>   the serde type.
     * @return the underlying {@link Serde serde} type.
     */
    public static <T, S extends Serde<T>> Class<T> underlyingSerdeType(Class<S> serde) {
        final int serdeTypeIndex = 0;
        final ResolvableType resolvableType = ResolvableType.forClass(serde)
                                                            .as(Serde.class);

        @SuppressWarnings("unchecked")
        final Class<T> serdeType = (Class<T>) resolvableType.resolveGeneric(serdeTypeIndex);
        return serdeType;
    }

    /**
     * Returns a {@link Serde#configure(Map, boolean) configured} instance of given Key Serde class.
     *
     * @param serdeClass the {@link Class} to instantiate and configure.
     * @param <S>        the resulting {@link Serde} type.
     * @param <T>        the underlying {@link Serde} type.
     * @return a {@link Serde#configure(Map, boolean) configured} instance of given Key Serde class.
     */
    public <T, S extends Serde<T>> Serde<T> keySerde(Class<S> serdeClass) {

        try {
            final Class<T> underlyingSerdeType = underlyingSerdeType(serdeClass);
            return Serdes.serdeFrom(underlyingSerdeType);
        } catch (IllegalArgumentException ignored) {}


        final String name = serdeClass.getName();
        try {
            final Map<String, Object> properties = valuesWithPrefixOverride(ADDITIONAL_SERDES_PROPERTIES_PREFIX);
            return getConfiguredInstance(name, serdeClass, properties);
        } catch (Exception e) {
            throw new AdditionalConfigException("Failed to configure Serde [" + name + "]:", e);
        }
    }

    /**
     * Validation for a list of fully qualified class names. Each class is expected to implement {@link Serde Serde}.
     */
    public static class ValidSerdesClassList implements Validator {

        private ValidSerdesClassList() { /* Intentionally empty constructor */ }

        /**
         * @return a new {@link ValidSerdesClassList} instance.
         */
        public static ValidSerdesClassList getInstance() {
            return new ValidSerdesClassList();
        }

        /**
         * Perform single configuration validation.
         *
         * @param name  The name of the configuration
         * @param value The value of the configuration
         * @throws ConfigException if the value is invalid.
         */
        @Override
        public void ensureValid(final String name, final Object value) {

            @SuppressWarnings("unchecked")
            final List<String> parsedList = (List<String>) value;

            for (String className : parsedList) {
                final Class<?> parsedClass = (Class<?>) parseType(name, className, Type.CLASS);
                final boolean isSerde = Serde.class.isAssignableFrom(parsedClass);
                if (!isSerde) {
                    final String messageTemplate =
                            "Class [%s] must be assignable from [org.apache.kafka.common.serialization.Serde]";
                    throw new ConfigException(name, value, String.format(messageTemplate, className));
                }
            }
        }
    }
}