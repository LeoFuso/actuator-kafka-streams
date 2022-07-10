package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query;

import java.io.Serializable;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Optional;

import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;

public interface RemoteQueryableStore<R extends RemoteQueryableStore<R>> extends Serializable, Remote {

    static RemoteQueryableReadOnlyKeyValueStore localReadOnly(BeanFactory factory) {
        return RemoteQueryableReadOnlyKeyValueStore.local(factory);
    }

    static RemoteQueryableReadOnlyKeyValueStore remoteReadOnly(BeanFactory factory) {
        return RemoteQueryableReadOnlyKeyValueStore.local(factory);
    }

    default void initialize() throws AlreadyBoundException, RemoteException {
        bind();
    }

    default void bind() throws AlreadyBoundException, RemoteException {
        final HostInfo info = info();
        final int port = info.port();

        final Registry registry = LocateRegistry.createRegistry(port);
        final String name = name();
        registry.bind(name, this);
    }

    HostInfo info();

    String name();

    default Optional<R> lookup(HostInfo info) throws NotBoundException, RemoteException {

        // TODO this lookup method should be a factory, holding all types of remote stores, and depending on the
        // host info, return either the local instance or the remote one.

        final HostInfo localInfo = this.info();
        final boolean localLookup = localInfo.equals(info);
        if (localLookup) {
            final R store = doLookUp();
            return Optional.ofNullable(store);
        }

        final RestTemplateBuilder builder = beanFactory().getBean(RestTemplateBuilder.class);
        final String host = info.host();
        final int port = info.port();

        final Registry remoteRegistry = LocateRegistry.getRegistry(host, port);
        final R store = doLookUp(remoteRegistry);
        return Optional.ofNullable(store);
    }

    BeanFactory beanFactory();

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
