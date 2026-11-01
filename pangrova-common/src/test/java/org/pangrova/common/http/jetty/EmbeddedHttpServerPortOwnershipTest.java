package org.pangrova.common.http.jetty;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddedHttpServerPortOwnershipTest {
    @Test
    void assignedEphemeralPortCannotBeClaimedByAnotherLoopbackListener() throws Exception {
        Server server = EmbeddedHttpServerFactory.create(new InetSocketAddress("127.0.0.1", 0),
                new ServletContextHandler(), 16);
        try {
            server.start();
            int port = EmbeddedHttpServerFactory.getPort(server);
            try (ServerSocketChannel other = ServerSocketChannel.open()) {
                other.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                assertThatThrownBy(() -> other.bind(new InetSocketAddress("127.0.0.1", port)))
                        .isInstanceOf(BindException.class);
            }
        } finally {
            server.stop();
            server.destroy();
        }
    }

    @Test
    void ephemeralBindingPolicyRejectsAnExistingAddressSpecificListener() throws Exception {
        try (ServerSocketChannel existing = ServerSocketChannel.open()) {
            existing.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            existing.bind(new InetSocketAddress("127.0.0.1", 0));
            int ownedPort = ((InetSocketAddress) existing.getLocalAddress()).getPort();
            Server server = EmbeddedHttpServerFactory.create(0, new ServletContextHandler());
            try {
                // Force the kernel's conflicting assignment deterministically without racing a released port.
                ((ServerConnector) server.getConnectors()[0]).setPort(ownedPort);
                assertThatThrownBy(server::start).isInstanceOf(java.io.IOException.class)
                        .hasRootCauseInstanceOf(BindException.class);
            } finally {
                // Failed startup has no running servlet context to drain gracefully.
                server.setStopTimeout(0);
                server.stop();
                server.destroy();
            }
        }
    }
}
