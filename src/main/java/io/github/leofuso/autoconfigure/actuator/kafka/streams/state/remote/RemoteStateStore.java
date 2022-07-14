package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;

import com.google.protobuf.ByteString;

/**
 * A {@link RemoteStateStore} encapsulates a {@link org.apache.kafka.streams.processor.StateStore store} capable of
 * receiving queries remotely.
 */
public interface RemoteStateStore {

    /**
     * @return the name to associate with this {@link RemoteStateStore} reference.
     */
    String reference();

    /**
     * @return a {@link HostInfo host} that points to itself.
     */
    default HostInfo self() {
        return HostInfo.unavailable();
    }

    /**
     * @param type to compare against.
     * @return either or not the given {@link QueryableStoreType type} is compatible with this {@link RemoteStateStore}
     * instance.
     */
    default boolean isCompatible(QueryableStoreType<?> type) {
        return types().contains(type.getClass());
    }

    /**
     * @return a {@link Set} containing all {@link QueryableStoreType} associated with this {@link RemoteStateStore}.
     */
    Set<Class<? extends QueryableStoreType<?>>> types();

    /**
     * @param host used to locate the {@link RemoteStateStore store}.
     * @param <R>  the wanted {@link RemoteStateStore store} type.
     * @return a stub for this {@link RemoteStateStore store}, or the real one, if a proxy creation is unnecessary.
     */
    <R extends RemoteStateStore> R stub(HostInfo host);

    default Integer getMethodKey() {
        return 0;
    }

    default Optional<Method> method(Invocation invocation) {

        final Integer methodKey = getMethodKey();
        final String toLookFor =
                invocation.getArgumentsOrDefault(methodKey, ByteString.EMPTY)
                          .toString(StandardCharsets.UTF_8);

        final Predicate<Method> methodPredicate = method -> {
            final String actual = method.getName();
            return actual.equals(toLookFor);
        };

        final Method[] methods = KeyValueStateStoreStub.class.getDeclaredMethods();
        return Arrays.stream(methods)
                     .filter(methodPredicate)
                     .findFirst();
    }

}
