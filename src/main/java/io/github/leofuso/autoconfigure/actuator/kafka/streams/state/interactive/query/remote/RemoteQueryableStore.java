package io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.remote;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.kafka.streams.state.HostInfo;

import io.github.leofuso.autoconfigure.actuator.kafka.streams.state.interactive.query.QueryableStore;


/**
 * Same as {@link QueryableStore}, but with the capability of being queried through a {@link Registry known registry}.
 */
public interface RemoteQueryableStore extends QueryableStore, Serializable, Remote {

    /**
     * Used to bind this {@link Remote instance} to a {@link java.rmi.registry.Registry registry}.
     */
    default void initialize() throws RemoteException {
        try {
            final String name = reference();
            final HostInfo self = self();
            final Registry registry = locateOrCreateRegistry(self);

            final int port = self.port();
            UnicastRemoteObject.exportObject(this, port);

            /* Always rebinding, since we don't need to worry about stale references. */
            registry.rebind(name, this);

        } catch (RemoteException e) {
            try {
                UnicastRemoteObject.unexportObject(this, true);
            } catch (NoSuchObjectException ignored) { /* Swallowing exception */ }
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the name to associate with this {@link Remote} reference.
     */
    String reference() throws RemoteException;

    /**
     * @return a {@link HostInfo host} that points to itself.
     */
    HostInfo self() throws RemoteException;

    /**
     * Locate or create the RMI registry for this store.
     *
     * @param host carrying the registry port to use
     * @return the RMI registry
     *
     * @throws RuntimeException if the registry couldn't be located or created.
     */
    default Registry locateOrCreateRegistry(HostInfo host) throws RemoteException{
        final int port = host.port();
        synchronized (LocateRegistry.class) {
            try {
                Registry registry = LocateRegistry.getRegistry(port);
                registry.list();
                return registry;
            } catch (RemoteException ex) {
                try {
                    return LocateRegistry.createRegistry(port);
                } catch (RemoteException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Unbind the RMI service from the registry on bean factory shutdown.
     */
    default void destroy() throws RemoteException {
        try {
            final Registry registry = locateOrCreateRegistry(self());
            registry.unbind(reference());
        } catch (NotBoundException | RemoteException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                UnicastRemoteObject.unexportObject(this, true);
            } catch (NoSuchObjectException ignored) { /* Swallowing exception */ }
        }
    }

    /**
     * @param host used to locate the {@link RemoteQueryableStore store}.
     * @param <R>  the type of the wanted {@link RemoteQueryableStore store}.
     * @return a stub for this {@link RemoteQueryableStore store}, or the real one, if a proxy creation is unnecessary.
     */
    @SuppressWarnings("unchecked")
    default <R extends RemoteQueryableStore> R stub(HostInfo host) throws RemoteException {
        try {

            final boolean unnecessaryStub = self().equals(host);
            if (unnecessaryStub) {
                return (R) this;
            }

            final String hostname = host.host();
            final int port = host.port();

            final Registry registry = LocateRegistry.getRegistry(hostname, port);

            final String reference = reference();
            return (R) registry.lookup(reference);

        } catch (RemoteException | NotBoundException e) {
            throw new RuntimeException(e);
        }
    }

}
