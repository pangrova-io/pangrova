package org.pangrova.common.http.jetty;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Factory for creating embedded Jetty servers.
 * Encapsulates Server + context handler + QueuedThreadPool setup.
 * Reference: Kafka Connect RestServer pattern.
 */
public final class EmbeddedHttpServerFactory {

    static final int DEFAULT_MAX_THREADS = 16;
    static final int MIN_THREADS = 2;
    static final long IDLE_TIMEOUT_MS = 5000;
    static final long SHUTDOWN_IDLE_TIMEOUT_MS = 100;
    static final long STOP_TIMEOUT_MS = 5000;

    private EmbeddedHttpServerFactory() {
        // utility class
    }

    /**
     * Creates a Jetty Server with default thread pool (maxThreads=16).
     * Server is NOT started — caller is responsible for calling server.start().
     *
     * @apiNote The default maxThreads=16 is suitable for lightweight admin/health endpoints only.
     *          Production query-serving endpoints should specify a higher maxThreads via
     *          {@link #create(int, ServletContextHandler, int)}.
     */
    public static Server create(int port, ServletContextHandler handler) {
        return create(port, handler, DEFAULT_MAX_THREADS);
    }

    /**
     * Creates a Jetty Server with the specified thread pool limit.
     * Server is NOT started — caller is responsible for calling server.start().
     */
    public static Server create(int port, ServletContextHandler handler, int maxThreads) {
        return create(new InetSocketAddress(port), handler, maxThreads);
    }

    /** Creates a server bound to an explicit address rather than an implicit wildcard. */
    public static Server create(InetSocketAddress address, ServletContextHandler handler, int maxThreads) {
        QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, MIN_THREADS);
        threadPool.setDaemon(true);
        Server server = new Server(threadPool);
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(address.getHostString());
        connector.setPort(address.getPort());
        // A kernel-assigned port has no restart identity to reuse. On BSD,
        // SO_REUSEADDR can otherwise overlap a live address-specific listener.
        if (address.getPort() == 0) {
            connector.setReuseAddress(false);
        }
        connector.setIdleTimeout(IDLE_TIMEOUT_MS);
        connector.setShutdownIdleTimeout(SHUTDOWN_IDLE_TIMEOUT_MS);
        server.addConnector(connector);
        server.setHandler(handler);
        server.setStopTimeout(STOP_TIMEOUT_MS);
        return server;
    }

    /** Returns the actual bound port after server start. */
    public static int getPort(Server server) {
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }
}
