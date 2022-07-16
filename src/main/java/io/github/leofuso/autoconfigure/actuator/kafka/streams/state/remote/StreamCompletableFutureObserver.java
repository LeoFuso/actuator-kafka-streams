package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Value;
import io.grpc.stub.StreamObserver;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.deserialize;

/**
 * A bridge between a {@link CompletableFuture} and a {@link StreamObserver}.
 *
 * @param <T> the type returned by the {@link CompletableFuture} part.
 */
public class StreamCompletableFutureObserver<T> extends CompletableFuture<T> implements StreamObserver<Value> {

    @Override
    public void onNext(final Value value) {
        final ByteString content = value.getContent();
        final byte[] serializedContent = content.toByteArray();
        final T result = deserialize(serializedContent);
        complete(result);
    }

    @Override
    public void onError(final Throwable throwable) {
        final Throwable underlyingCause = throwable.getCause();
        if (underlyingCause != null) {
            completeExceptionally(throwable);
        }
        completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() { /* empty */ }

}
