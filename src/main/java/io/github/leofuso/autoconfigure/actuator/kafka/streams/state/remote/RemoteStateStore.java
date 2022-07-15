package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.util.ReflectionUtils;

import com.google.protobuf.ByteString;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.deserialize;

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

    default Optional<Method> method(Invocation invocation) {

        final Integer methodKey = methodKey();
        final ByteString methodArgument = invocation.getArgumentsOrDefault(methodKey, ByteString.EMPTY);
        final byte[] bytes = methodArgument.toByteArray();

        final String methodOfInterest = deserialize(bytes);
        final Predicate<Method> methodPredicate = method -> {
            final String currentMethod = method.getName();
            return currentMethod.equals(methodOfInterest);
        };

        final Method[] methods = getClass().getMethods();
        return Arrays.stream(methods)
                     .filter(methodPredicate)
                     .findFirst();
    }

    default Integer methodKey() {
        return 0;
    }

    default CompletableFuture<?> invoke(Method method, Invocation invocation) {

        final Predicate<Map.Entry<Integer, ByteString>> methodNameFilter = entry -> {
            final Integer methodKey = methodKey();
            final Integer currentKey = entry.getKey();
            return !currentKey.equals(methodKey);
        };

        final Object[] args = invocation
                .getArgumentsMap()
                .entrySet()
                .stream()
                .filter(methodNameFilter)
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .map(ByteString::toByteArray)
                .map(SerializationUtils::deserialize)
                .toArray();

        return (CompletableFuture<?>) ReflectionUtils.invokeMethod(method, this, args);
    }
}
