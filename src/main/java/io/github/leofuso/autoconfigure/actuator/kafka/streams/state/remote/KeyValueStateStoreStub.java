package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.StateStoreGrpc.StateStoreStub;

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
        this.host = host;
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
        CompletableFuture<V> completable = new CompletableFuture<>();
        StreamObserver<Value> observer = new StreamObserver<>() {

            @Override
            public void onNext(Value value) {
                @SuppressWarnings("unchecked")
                final V content = (V) value.getContent();
                completable.complete(content);
            }

            @Override
            public void onError(Throwable throwable) {
                completable.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {}
        };

        final Invocation invocation =
                Invocation.newBuilder()
                          .setStore(reference())
                          .putArguments(KEY_ARG, ByteString.copyFrom((byte[]) key))
                          .putArguments(STORE_ARG, ByteString.copyFrom(storeName, StandardCharsets.UTF_8))
                          .putArguments(getMethodKey(), ByteString.copyFrom(method, StandardCharsets.UTF_8))
                          .build();

        stub.invoke(invocation, observer);
        return completable;
    }

    @Override
    public HostInfo self() {
        return host;
    }
}
