package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import com.google.protobuf.ByteString;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Invocation;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.StateStoreGrpc.StateStoreStub;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.serialize;

/**
 * A stub to receive and delegate invocations to the actual {@link RemoteKeyValueStateStore store}, remotely located.
 */
public class KeyValueStateStoreStub implements RemoteKeyValueStateStore {

    /**
     * The key argument position.
     */
    private static final Integer KEY_ARG = 0;

    /**
     * The storeName argument position.
     */
    private static final Integer STORE_ARG = 1;

    /**
     * The {@link StateStoreStub StateStoreStub} that all invocations will be delegated to.
     */
    private final StateStoreStub stub;

    /**
     * The {@link HostInfo host} that point out to this {@link RemoteKeyValueStateStore store}.
     */
    private final HostInfo host;

    /**
     * Constructs a new {@link KeyValueStateStoreStub stub} instance to carry out the method invocations that a
     * {@link RemoteKeyValueStateStore} can receive.
     *
     * @param stub that delegates the invocations.
     * @param host of the exposed {@link RemoteKeyValueStateStore store}.
     */
    public KeyValueStateStoreStub(final StateStoreStub stub, final HostInfo host) {
        this.stub = Objects.requireNonNull(stub, "Field [stub] is required.");
        this.host = Objects.requireNonNull(host, "Field [host] is required.");
    }

    @Override
    public <K, V> CompletableFuture<V> findOne(final K key, final String storeName) {
        return doFindOne("findOne", key, storeName);
    }

    @Override
    public <K, V> CompletableFuture<ValueAndTimestamp<V>> findOneTimestamped(final K key, final String storeName) {
        return doFindOne("findOneTimestamped", key, storeName);
    }

    private <K, V> CompletableFuture<V> doFindOne(String method, final K key, final String storeName) {
        final StreamCompletableFutureObserver<V> observer = new StreamCompletableFutureObserver<>();
        final Invocation invocation =
                Invocation.newBuilder()
                          .setStoreReference(reference())
                          .putArguments(KEY_ARG, ByteString.copyFrom(serialize(key)))
                          .putArguments(STORE_ARG, ByteString.copyFrom(serialize(storeName)))
                          .putArguments(methodKey(), ByteString.copyFrom(serialize(method)))
                          .build();

        stub.invoke(invocation, observer);
        return observer;
    }

    @Override
    public HostInfo self() {
        return host;
    }
}
