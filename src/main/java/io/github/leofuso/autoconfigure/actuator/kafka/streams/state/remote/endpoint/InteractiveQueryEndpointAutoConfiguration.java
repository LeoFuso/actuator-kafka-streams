package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.remote.endpoint;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.ConversionService;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import com.google.protobuf.Service;

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


@AutoConfiguration(
        before = {CompositeStateAutoConfiguration.class},
        after = {EndpointAutoConfiguration.class, KafkaStreamsDefaultConfiguration.class}
)
@ConditionalOnClass({Server.class, Service.class, StreamObserver.class, Endpoint.class})
@ConditionalOnBean({StreamsBuilderFactoryBean.class})
@ConditionalOnProperty(prefix = "spring.kafka", name = {"streams.properties." + APPLICATION_SERVER_CONFIG})
public class InteractiveQueryEndpointAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(GrpcChannelConfigurer.class)
    public GrpcChannelConfigurer plainTextChannelConfigurer() {
        return new PlainTextChannelConfigurer();
    }

    @Bean
    @ConditionalOnMissingBean(LocalKeyValueStore.class)
    @ConditionalOnAvailableEndpoint(endpoint = ReadOnlyStateStoreEndpoint.class)
    public RemoteStateStore remoteKeyValueStateStore(ObjectProvider<StreamsBuilderFactoryBean> provider) {
        final StreamsBuilderFactoryBean factory = provider.getIfAvailable();
        if (factory != null) {
            return new LocalKeyValueStore(factory);
        }
        return null;
    }

    @Bean(destroyMethod = "cleanUp")
    @ConditionalOnMissingBean(HostManager.class)
    public HostManager hostManager(ObjectProvider<StreamsBuilderFactoryBean> provider,
                                   ObjectProvider<RemoteStateStore> stores,
                                   ObjectProvider<GrpcChannelConfigurer> configurers) {

        final StreamsBuilderFactoryBean factory = provider.getIfAvailable();
        if (factory != null) {
            return new DefaultHostManager(factory, stores.orderedStream(), configurers.orderedStream());
        }
        return null;
    }

    @Bean
    @ConditionalOnMissingBean(HostManager.CleanUpListener.class)
    public HostManager.CleanUpListener hostManagerCleanUpListener(ObjectProvider<HostManager> provider) {
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
    public Server gRpcStateStoreServer(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider,
                                       ObjectProvider<GrpcServerConfigurer> configurers,
                                       ObjectProvider<HostManager> managerProvider) {

        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        final HostManager manager = managerProvider.getIfAvailable();

        return ofNullable(factory)
                .filter(f -> manager != null)
                .map(StreamsBuilderFactoryBean::getStreamsConfiguration)
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
    public RemoteQuerySupport remoteQuerySupport(ObjectProvider<StreamsBuilderFactoryBean> factoryProvider,
                                                 ObjectProvider<HostManager> managerProvider,
                                                 ObjectProvider<ConversionService> converterProvider) {

        final StreamsBuilderFactoryBean factory = factoryProvider.getIfAvailable();
        final HostManager manager = managerProvider.getIfAvailable();
        final ConversionService converter = converterProvider.getIfAvailable();

        if (factory != null && manager != null && converter != null) {
            return new DefaultRemoteQuerySupport(manager, converter, factory.getStreamsConfiguration());
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
