package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import static io.github.leofuso.autoconfigure.actuator.kafka.streams.utils.SerializationUtils.serialize;

public class StateStoreService extends StateStoreGrpc.StateStoreImplBase {

    private final RemoteQuerySupport support;

    public StateStoreService(RemoteQuerySupport support) {
        this.support = Objects.requireNonNull(support, "Field [support] is required.");
    }

    public static Server getServerInstance(StreamsBuilderFactoryBean factory, RemoteQuerySupport support) {
        return Optional.of(factory)
                       .map(StreamsBuilderFactoryBean::getStreamsConfiguration)
                       .map(StreamsConfig::new)
                       .map(config -> config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG))
                       .map(HostInfo::buildFromEndpoint)
                       .map(info -> {

                           final StateStoreService service = new StateStoreService(support);

                           final int port = info.port();
                           return ServerBuilder.forPort(port)
                                               .addService(service)
                                               .build();
                       })
                       .orElseThrow();
    }

    @Override
    public void invoke(final Invocation invocation, final StreamObserver<Value> observer) {

        final String reference = invocation.getStoreReference();
        final Optional<RemoteStateStore> storeFound = support.findStore(reference);
        final boolean missingStore = storeFound.isEmpty();
        if (missingStore) {
            /* Some specific Exception */
            final IllegalStateException exception = new IllegalStateException();
            observer.onError(exception);
            return;
        }

        final RemoteStateStore store = storeFound.get();
        final Optional<Method> methodFound = store.method(invocation);
        final boolean missingMethod = methodFound.isEmpty();
        if (missingMethod) {
            /* Some specific Exception */
            final IllegalStateException exception = new IllegalStateException();
            observer.onError(exception);
            return;
        }

        final Method method = methodFound.get();
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
