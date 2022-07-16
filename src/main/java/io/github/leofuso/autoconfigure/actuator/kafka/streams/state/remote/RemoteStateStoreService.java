package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import com.google.protobuf.ByteString;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions.MissingMethodException;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.exceptions.MissingStoreException;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Invocation;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.StateStoreGrpc;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.serialize;

public class RemoteStateStoreService extends StateStoreGrpc.StateStoreImplBase {

    private final RemoteQuerySupport support;

    public RemoteStateStoreService(RemoteQuerySupport support) {
        this.support = Objects.requireNonNull(support, "Field [support] is required.");
    }

    /**
     * Creates a new instance of this {@link RemoteStateStoreService service}. Will throw a {@link NullPointerException}
     * if missing required configurations, including the ability to create a {@link HostInfo host}, necessary to expose
     * this {@link RemoteStateStoreService service}.
     *
     * @param factory needed to extract a {@link HostInfo}.
     * @param support to delegate the {@link Invocation invocations} to.
     * @return a new {@link Server server} ready to be started.
     */
    public static Server getServerInstance(StreamsBuilderFactoryBean factory, RemoteQuerySupport support) {
        return Optional.of(factory)
                       .map(StreamsBuilderFactoryBean::getStreamsConfiguration)
                       .map(StreamsConfig::new)
                       .map(config -> config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG))
                       .map(HostInfo::buildFromEndpoint)
                       .map(info -> {

                           final RemoteStateStoreService service = new RemoteStateStoreService(support);

                           final int port = info.port();
                           return ServerBuilder.forPort(port)
                                               .addService(service)
                                               .build();
                       })
                       .orElseThrow(() -> {
                           final String message =
                                   "KafkaStreams has not been started, or hasn't been configured. Try again later.";
                           return new StreamsNotStartedException(message);
                       });
    }

    @Override
    public void invoke(final Invocation invocation, final StreamObserver<Value> observer) {

        final String reference = invocation.getStoreReference();
        final Optional<RemoteStateStore> storeFound = support.findStore(reference);
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
