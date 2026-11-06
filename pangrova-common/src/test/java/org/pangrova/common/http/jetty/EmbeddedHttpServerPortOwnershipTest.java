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
    void concreteBindingRejectsAnotherListenerOnTheSameAddress() throws Exception {
        try (ServerSocketChannel existing = ServerSocketChannel.open()) {
            existing.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            existing.bind(new InetSocketAddress("127.0.0.1", 0));
            Server server = EmbeddedHttpServerFactory.create(
                    (InetSocketAddress) existing.getLocalAddress(), new ServletContextHandler(), 16);
            try {
                assertThatThrownBy(server::start).isInstanceOf(java.io.IOException.class)
                        .hasRootCauseInstanceOf(BindException.class);
            } finally {
                server.setStopTimeout(0);
                server.stop();
                server.destroy();
            }
        }
    }

    @Test
    void concreteAddressCanRestartAfterServingARealRequest() throws Exception {
        Server first = respondingServer(0);
        int port;
        try {
            first.start();
            port = EmbeddedHttpServerFactory.getPort(first);
            requireResponse(port);
        } finally {
            first.stop();
            first.destroy();
        }
        Server replacement = respondingServer(port);
        try {
            replacement.start();
            requireResponse(port);
        } finally {
            replacement.stop();
            replacement.destroy();
        }
    }

    private static Server respondingServer(int port) {
        var handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new org.eclipse.jetty.servlet.ServletHolder(new jakarta.servlet.http.HttpServlet() {
            @Override
            protected void doGet(jakarta.servlet.http.HttpServletRequest request,
                    jakarta.servlet.http.HttpServletResponse response) {
                response.setStatus(204);
                response.setHeader("Connection", "close");
            }
        }), "/*");
        return EmbeddedHttpServerFactory.create(new InetSocketAddress("127.0.0.1", port), handler, 16);
    }

    private static void requireResponse(int port) throws Exception {
        try (var socket = new java.net.Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            String response = new String(socket.getInputStream().readAllBytes(),
                    java.nio.charset.StandardCharsets.US_ASCII);
            org.assertj.core.api.Assertions.assertThat(response).startsWith("HTTP/1.1 204");
        }
    }

    @Test
    void fixedWildcardBindingRejectsAnExistingAddressSpecificListener() throws Exception {
        try (ServerSocketChannel existing = ServerSocketChannel.open()) {
            existing.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            existing.bind(new InetSocketAddress("127.0.0.1", 0));
            int ownedPort = ((InetSocketAddress) existing.getLocalAddress()).getPort();
            Server server = EmbeddedHttpServerFactory.create(ownedPort, new ServletContextHandler());
            try {
                assertThatThrownBy(server::start).isInstanceOf(java.io.IOException.class)
                        .hasRootCauseInstanceOf(BindException.class);
            } finally {
                server.setStopTimeout(0);
                server.stop();
                server.destroy();
            }
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
