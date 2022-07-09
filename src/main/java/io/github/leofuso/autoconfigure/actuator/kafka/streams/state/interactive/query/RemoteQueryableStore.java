package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.io.Serializable;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Optional;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.SmartInitializingSingleton;

public interface RemoteQueryableStore<K, V, R extends RemoteQueryableStore<K, V, R>>
        extends Serializable, Remote, SmartInitializingSingleton {

    static <K, V> RemoteQueryableReadOnlyKeyValueStore<K, V> readOnly(StreamsConfig config, KafkaStreams streams) {
        return RemoteQueryableReadOnlyKeyValueStore.instantiate(config, streams);
    }

    @Override
    default void afterSingletonsInstantiated() {
        try {
            bind();
        } catch (AlreadyBoundException | RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    String name();

    HostInfo info();

    default void bind() throws AlreadyBoundException, RemoteException {
        final HostInfo info = info();
        final int port = info.port();

        final Registry registry = LocateRegistry.createRegistry(port);
        final String name = name();
        registry.bind(name, this);
    }

    default Optional<R> lookup(HostInfo info) throws NotBoundException, RemoteException {
            final HostInfo localInfo = this.info();
            final boolean localLookup = localInfo.equals(info);
            if (localLookup) {
                final R store = doLookUp();
                return Optional.ofNullable(store);
            }

            final String host = info.host();
            final int port = info.port();

            final Registry remoteRegistry = LocateRegistry.getRegistry(host, port);
            final R store = doLookUp(remoteRegistry);
            return Optional.ofNullable(store);
    }


    private R doLookUp() throws RemoteException, NotBoundException {
        final int port = info().port();
        final Registry registry = LocateRegistry.getRegistry(port);
        return doLookUp(registry);
    }

    @SuppressWarnings("unchecked")
    private R doLookUp(Registry registry) throws NotBoundException, RemoteException {
        final String name = name();
        return (R) registry.lookup(name);
    }


}
