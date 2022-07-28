package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint;

import java.util.Objects;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.ConversionService;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import com.google.protobuf.Service;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.KStreamsSupplier;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.KStreamsSupplierAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.CompositeStateAutoConfiguration;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.DefaultHostManager;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.DefaultRemoteQuerySupport;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.HostManager;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.LocalKeyValueStore;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteQuerySupport;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStore;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.RemoteStateStoreService;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.GrpcChannelConfigurer;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.GrpcServerConfigurer;
import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.grpc.PlainTextChannelConfigurer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import static java.util.Optional.ofNullable;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;


/**
 * {@link EnableAutoConfiguration Auto-configuration} for all interactive queries functionality.
 */
@AutoConfiguration(
        before = {CompositeStateAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class},
        after = {EndpointAutoConfiguration.class, KStreamsSupplierAutoConfiguration.class}
)
@ConditionalOnClass({Server.class, Service.class, StreamObserver.class, Endpoint.class})
@ConditionalOnBean({KStreamsSupplier.class})
@ConditionalOnProperty(prefix = "spring.kafka", name = {"streams.properties." + APPLICATION_SERVER_CONFIG})
public class InteractiveQueryEndpointAutoConfiguration {

    /**
     * A naive supplier of {@link org.apache.kafka.streams.KafkaStreams KafkaStreams}.
     */
    private final KStreamsSupplier streamsSupplier;

    public InteractiveQueryEndpointAutoConfiguration(final KStreamsSupplier streamsSupplier) {
        this.streamsSupplier = Objects.requireNonNull(streamsSupplier, "KStreamsSupplier [supplier] is required.");
    }

    @Bean
    @ConditionalOnMissingBean(GrpcChannelConfigurer.class)
    public GrpcChannelConfigurer plainTextChannelConfigurer() {
        return new PlainTextChannelConfigurer();
    }

    @Bean
    @ConditionalOnMissingBean(LocalKeyValueStore.class)
    @ConditionalOnAvailableEndpoint(endpoint = ReadOnlyStateStoreEndpoint.class)
    public RemoteStateStore remoteKeyValueStateStore() {
        return new LocalKeyValueStore(streamsSupplier);
    }

    @Bean(destroyMethod = "cleanUp")
    @ConditionalOnMissingBean(HostManager.class)
    public HostManager hostManager(ObjectProvider<RemoteStateStore> stores,
                                   ObjectProvider<GrpcChannelConfigurer> configurers) {
        return new DefaultHostManager(streamsSupplier, stores.orderedStream(), configurers.orderedStream());
    }

    @Bean
    @ConditionalOnMissingBean(HostManager.CleanUpListener.class)
    public KafkaStreams.StateListener hostManagerCleanUpListener(ObjectProvider<HostManager> provider) {
        final HostManager manager = provider.getIfAvailable();
        if (manager != null) {
            return new HostManager.CleanUpListener(manager);
        }
        return null;
    }


    @Bean(
            initMethod = "start",
            destroyMethod = "shutdown"
    )
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint(endpoint = ReadOnlyStateStoreEndpoint.class)
    public Server gRpcStateStoreServer(ObjectProvider<GrpcServerConfigurer> configurers,
                                       ObjectProvider<HostManager> provider) {

        final HostManager manager = provider.getIfAvailable();

        return ofNullable(manager)
                .map(m -> streamsSupplier.configAsProperties())
                .map(StreamsConfig::new)
                .map(config -> config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG))
                .map(HostInfo::buildFromEndpoint)
                .map(info -> {

                    final RemoteStateStoreService service = new RemoteStateStoreService(manager);
                    final int port = info.port();
                    final ServerBuilder<?> builder =
                            configurers.orderedStream()
                                       .reduce(

                                               ServerBuilder.forPort(port)
                                                            .addService(service),

                                               (ServerBuilder<?> acc, GrpcServerConfigurer config) ->
                                                       config.configure(acc),

                                               (b1, b2) -> b2
                                       );
                    return builder.build();

                })
                .orElseThrow(() -> new IllegalStateException("A required config is missing [application.server]."));
    }

    @Bean
    @ConditionalOnMissingBean(RemoteQuerySupport.class)
    public RemoteQuerySupport remoteQuerySupport(ObjectProvider<HostManager> managerProvider,
                                                 ObjectProvider<ConversionService> converterProvider) {

        final HostManager manager = managerProvider.getIfAvailable();
        final ConversionService converter = converterProvider.getIfAvailable();

        if (manager != null && converter != null) {
            return new DefaultRemoteQuerySupport(manager, converter, streamsSupplier.configAsProperties());
        }

        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint(endpoint = ReadOnlyStateStoreEndpoint.class)
    public ReadOnlyStateStoreEndpoint readOnlyStateStoreEndpoint(ObjectProvider<RemoteQuerySupport> provider) {
        final RemoteQuerySupport support = provider.getIfAvailable();
        if (support != null) {
            return new ReadOnlyStateStoreEndpoint(support);
        }
        return null;
    }
}
