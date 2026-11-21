package org.pangrova.transport.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates EmbeddedHttpServerFactory thread pool configuration.
 * Ensures Coordinator (256) and Worker (64) limits are correctly applied.
 */
class EmbeddedHttpServerFactoryThreadPoolTest {

    private Server server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void defaultCreateUsesDefaultMaxThreads() {
        server = EmbeddedHttpServerFactory.create(0, emptyHandler());
        QueuedThreadPool pool = (QueuedThreadPool) server.getThreadPool();
        assertThat(pool.getMaxThreads()).isEqualTo(EmbeddedHttpServerFactory.DEFAULT_MAX_THREADS);
    }

    @Test
    void coordinatorThreadPoolHas256MaxThreads() {
        server = EmbeddedHttpServerFactory.create(0, emptyHandler(), 256);
        QueuedThreadPool pool = (QueuedThreadPool) server.getThreadPool();
        assertThat(pool.getMaxThreads()).isEqualTo(256);
    }

    @Test
    void workerThreadPoolHas64MaxThreads() {
        server = EmbeddedHttpServerFactory.create(0, emptyHandler(), 64);
        QueuedThreadPool pool = (QueuedThreadPool) server.getThreadPool();
        assertThat(pool.getMaxThreads()).isEqualTo(64);
    }

    @Test
    void minThreadsAlwaysTwo() {
        server = EmbeddedHttpServerFactory.create(0, emptyHandler(), 128);
        QueuedThreadPool pool = (QueuedThreadPool) server.getThreadPool();
        assertThat(pool.getMinThreads()).isEqualTo(EmbeddedHttpServerFactory.MIN_THREADS);
    }

    @Test
    void connectorUsesShortShutdownIdleTimeout() {
        server = EmbeddedHttpServerFactory.create(0, emptyHandler(), 128);
        ServerConnector connector = (ServerConnector) server.getConnectors()[0];
        assertThat(connector.getIdleTimeout()).isEqualTo(EmbeddedHttpServerFactory.IDLE_TIMEOUT_MS);
        assertThat(connector.getShutdownIdleTimeout()).isEqualTo(
                EmbeddedHttpServerFactory.SHUTDOWN_IDLE_TIMEOUT_MS);
    }

    private static ServletContextHandler emptyHandler() {
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        return handler;
    }
}
