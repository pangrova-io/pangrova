package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.client.transport.http.PangrovaRestClient;
import org.pangrova.client.session.RetryPolicy;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** JDBC validity against actual health HTTP statuses; no deployed Coordinator readiness claim. */
class JdbcHealthProtocolTest {
    @Test
    void exactReadyStatusMakesAnOpenConnectionValid() throws Exception {
        try (var peer = new HealthPeer(200); var connection = peer.connect()) {
            assertThat(connection.isValid(2)).isTrue();
            connection.close();
            assertThat(connection.isValid(2)).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {201, 202, 204, 206, 301, 302, 307, 400, 401, 403, 404, 429, 500, 503})
    void nonReadyHealthCannotValidateTheConnection(int status) throws Exception {
        try (var peer = new HealthPeer(status); var connection = peer.connect()) {
            assertThat(connection.isValid(2)).isFalse();
        }
    }

    private static final class HealthPeer implements AutoCloseable {
        private final ExecutorService executor = ManagedExecutorFactory.newSingleThreadExecutor("jdbc-health-protocol");
        private HttpServer server;

        private HealthPeer(int status) throws Exception {
            try {
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.setExecutor(executor);
                server.createContext("/v1/health", exchange -> {
                    try (exchange) {
                        exchange.getRequestBody().readAllBytes();
                        byte[] body = ("{\"ready\":" + (status == 200) + "}").getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(status, status == 204 ? -1 : body.length);
                        if (status != 204) exchange.getResponseBody().write(body);
                    }
                });
                server.start();
            } catch (Exception | Error failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        private PangrovaConnection connect() {
            return new PangrovaConnection(PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                    .coordinatorPort(server.getAddress().getPort()).httpExecutorThreads(1).retryPolicy(RetryPolicy.NONE).build(), null);
        }

        @Override public void close() {
            if (server != null) server.stop(0);
            ExecutorDrain.shutdownAndAwait(executor, 5_000);
        }
    }
}
