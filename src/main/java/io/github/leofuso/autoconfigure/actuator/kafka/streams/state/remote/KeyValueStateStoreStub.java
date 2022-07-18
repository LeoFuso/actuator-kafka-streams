package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import com.google.protobuf.ByteString;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Invocation;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.StateStoreGrpc.StateStoreStub;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.StateStoreGrpc.newStub;
import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.serialize;

/**
 * A stub to receive and delegate invocations to the actual {@link RemoteKeyValueStateStore store}, remotely located.
 */
public class KeyValueStateStoreStub implements RemoteKeyValueStateStore, RemoteStateStoreStub {

    /**
     * The key argument position.
     */
    private static final Integer KEY_ARG = 0;

    /**
     * The storeName argument position.
     */
    private static final Integer STORE_ARG = 1;

    /**
     * Used to create a {@link io.grpc.Channel channel} to its {@link HostInfo host}.
     */
    private final ManagedChannelBuilder<?> builder;

    /**
     * Indicates either or not this {@link RemoteStateStoreStub stub} was properly initialized and is ready to receive
     * invocations.
     */
    private boolean initialized = false;


    /**
     * The {@link StateStoreStub StateStoreStub} that all invocations will be delegated to.
     */
    @Nullable
    private StateStoreStub stub;

    /**
     * The {@link HostInfo host} that point out to this {@link RemoteKeyValueStateStore store}.
     */
    @Nullable
    private HostInfo host;

    /**
     * Constructs a new {@link KeyValueStateStoreStub stub} instance to carry out the method invocations that a
     * {@link RemoteKeyValueStateStore} can receive.
     * <p>
     * To be properly utilized, this {@link KeyValueStateStoreStub stub} instance needs to be
     * {@link RemoteStateStoreStub#initialize() initialized} first.
     *
     * @param builder that can be further configured via {@link RemoteStateStoreStub#configure(Function) configure}
     *                method.
     */
    public KeyValueStateStoreStub(final ManagedChannelBuilder<?> builder) {
        this.builder = Objects.requireNonNull(builder, "Field [builder] is required.");
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
        if (!initialized) {
            throw new IllegalStateException("Stub not yet initialized. Please invoke initialize().");
        }

        final StreamCompletableFutureObserver<V> observer = new StreamCompletableFutureObserver<>();
        final Invocation invocation =
                Invocation.newBuilder()
                          .setStoreReference(reference())
                          .putArguments(KEY_ARG, ByteString.copyFrom(serialize(key)))
                          .putArguments(STORE_ARG, ByteString.copyFrom(serialize(storeName)))
                          .putArguments(methodKey(), ByteString.copyFrom(serialize(method)))
                          .build();

        Objects.requireNonNull(stub, "Stub shouldn't be null.");
        stub.invoke(invocation, observer);
        return observer;
    }

    @Override
    public HostInfo self() {
        if (!initialized) {
            throw new IllegalStateException("Stub not yet initialized. Please invoke initialize().");
        }
        return host;
    }

    @Override
    public void configure(Function<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> configuration) {
        configuration.apply(builder);
    }

    @Override
    public void initialize() {
        if (initialized) {
            throw new IllegalStateException("You can't initialize a stub twice.");
        }
        final ManagedChannel channel = builder.build();
        final String authority = channel.authority();
        host = HostInfo.buildFromEndpoint(authority);
        stub = newStub(channel);
        initialized = true;
    }

    @Override
    public void shutdown() throws InterruptedException {
        final Duration now = Duration.ofMillis(0);
        shutdown(now);
    }

    @Override
    public void shutdown(final Duration timeout) throws InterruptedException {
        if (!initialized) {
            throw new IllegalStateException("You can't invoke a shutdown before the stub initialization.");
        }

        if (stub != null && stub.getChannel() instanceof ManagedChannel) {
            final ManagedChannel channel = (ManagedChannel) stub.getChannel();
            channel.shutdown();
            channel.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
