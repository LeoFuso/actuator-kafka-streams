package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;

class HttpQueryableReadOnlyKeyValueStore implements RemoteQueryableReadOnlyKeyValueStore {

    private final BeanFactory beanFactory;
    private final HostInfo info;

    private final RestTemplateBuilder builder;

    HttpQueryableReadOnlyKeyValueStore(final BeanFactory beanFactory, final HostInfo info) {
        this.beanFactory = Objects.requireNonNull(beanFactory, "BeanFactory [beanFactory] is required.");
        this.info = Objects.requireNonNull(info, "HostInfo [info] is required.");

        builder = beanFactory.getBean(RestTemplateBuilder.class);
    }

    @Override
    public <K, V> Optional<V> findByKey(final K key, final String storeName) {
        if(key instanceof String stringKey) {

        }
        final String keyClassName = key.getClass().getName();

        final String host = info.host();
        final int port = info.port();
        /*
         * TODO We'll need to check the actuator's endpoint configuration to resolve this path. :(
         * Also, it's probably best to change the strategy and drop the use of the BeanFactory.
         * ... trying GoF Strategy, maybe? Let the user decide how it will interact with the Remote interface?
         * Via HTTP or XMS? Offers a HTTP default implementation, with a RestTemplate bean.
         *
         * I'll need to drop the static factories, but I think it's for the best. The code is getting quite confuse.
         * Oh well........
         *
         */
        builder.rootUri(host + port + "/actuator/");
        return Optional.empty();
    }

    @Override
    public <K, V> Optional<KeyQueryMetadata> queryMetadataForKey(final K key, final String storeName) {
        return Optional.empty();
    }

    @Override
    public HostInfo info() {
        return info;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public BeanFactory beanFactory() {
        return beanFactory;
    }
}
