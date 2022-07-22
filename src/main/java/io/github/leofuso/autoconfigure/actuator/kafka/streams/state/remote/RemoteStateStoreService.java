package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions.MissingMethodException;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions.MissingStoreException;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Invocation;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.StateStoreGrpc;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Value;
import io.grpc.stub.StreamObserver;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.serialize;

public class RemoteStateStoreService extends StateStoreGrpc.StateStoreImplBase {

    private final HostManager manager;

    public RemoteStateStoreService(HostManager manager) {
        this.manager = Objects.requireNonNull(manager, "Field [manager] is required.");
    }

    @Override
    public void invoke(final Invocation invocation, final StreamObserver<Value> observer) {

        final String reference = invocation.getStoreReference();
        final Optional<RemoteStateStore> storeFound = manager.findStore(reference);
        final boolean missingStore = storeFound.isEmpty();
        if (missingStore) {
            final MissingStoreException exception = new MissingStoreException(reference);
            observer.onError(exception);
            return;
        }

        final RemoteStateStore store = storeFound.get();

        final Method method;
        try {
            method = store.method(invocation);
        } catch (MissingMethodException ex) {
            observer.onError(ex);
            return;
        }

        final CompletableFuture<?> future = store.invoke(method, invocation);
        final Object result = future.getNow(null);
        final byte[] bytes = serialize(result);

        final ByteString content = ByteString.copyFrom(bytes);
        final Value value = Value.newBuilder()
                                 .setContent(content)
                                 .build();
        observer.onNext(value);
        observer.onCompleted();
    }
}
