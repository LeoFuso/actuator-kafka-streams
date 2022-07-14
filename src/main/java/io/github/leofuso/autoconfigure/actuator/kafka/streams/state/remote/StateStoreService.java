package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.SerializationUtils;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

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

        final String store = invocation.getStore();

        final Optional<RemoteStateStore> storeFound = support.findStore(store);
        final boolean missingStore = storeFound.isEmpty();
        if (missingStore) {
            /* Some specific Exception */
            final IllegalStateException exception = new IllegalStateException();
            observer.onError(exception);
            return;
        }

        final RemoteStateStore stateStore = storeFound.get();
        final Optional<Method> methodFound = stateStore.method(invocation);
        final boolean missingMethod = methodFound.isEmpty();
        if (missingMethod) {
            /* Some specific Exception */
            final IllegalStateException exception = new IllegalStateException();
            observer.onError(exception);
            return;
        }

        final Method method = methodFound.get();
        final Object[] args = invocation
                .getArgumentsMap()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .map(ByteString::toByteArray)
                .map(SerializationUtils::deserialize)
                .toArray();

        final Object result = ReflectionUtils.invokeMethod(method, stateStore, args);
        final byte[] serializedResult = SerializationUtils.serialize(result);
        Assert.notNull(serializedResult, "SerializedResult cannot be null.");

        final ByteString content = ByteString.copyFrom(serializedResult);
        final Value value = Value.newBuilder()
                                 .setContent(content)
                                 .build();
        observer.onNext(value);
        observer.onCompleted();
    }
}
