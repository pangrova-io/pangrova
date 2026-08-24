package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.client.rest.PangrovaRestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies BUG-D41 fix: {@link PangrovaStatement#cancel()} delegates to
 * {@code PangrovaRestClient.cancelQuery(queryId)} for the currently executing query.
 *
 * <p>Uses an embedded HTTP server as a test double (same pattern as
 * {@link PangrovaStatementCancelOnDisconnectTest}). No mock frameworks.
 */
@DisplayName("PangrovaStatement.cancel() (D41)")
class PangrovaStatementCancelTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("cancel() sends cancelQuery to coordinator for the in-flight query")
    void cancelSendsCancelRequestDuringExecution() throws Exception {
        String queryId = "q-d41-cancel-001";
        AtomicBoolean cancelReceived = new AtomicBoolean(false);
        AtomicReference<String> cancelledQueryId = new AtomicReference<>();
        CountDownLatch querySubmitted = new CountDownLatch(1);
        CountDownLatch cancelDone = new CountDownLatch(1);

        // Submit endpoint: return RUNNING so the statement enters the poll loop
        server.createContext("/v1/query/submit", exchange -> {
            String body = """
                {
                  "query_id": "%s",
                  "state": "RUNNING",
                  "column_names": []
                }
                """.formatted(queryId);
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
            querySubmitted.countDown();
        });

        // Status endpoint: keep returning RUNNING to hold the statement in the poll loop
        server.createContext("/v1/query/" + queryId, exchange -> {
            String body = """
                {
                  "query_id": "%s",
                  "state": "RUNNING"
                }
                """.formatted(queryId);
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });

        // Cancel endpoint: record that cancel was called with the correct queryId
        server.createContext("/v1/query/" + queryId + "/cancel", exchange -> {
            cancelReceived.set(true);
            cancelledQueryId.set(queryId);
            String body = "{\"outcome\":\"CANCEL_ACCEPTED\"}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(202, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
            cancelDone.countDown();
        });

        server.start();

        PangrovaStatement stmt = createStatement();

        // Execute query in a background thread (it will block in the poll loop)
        CompletableFuture<Void> queryFuture = CompletableFuture.runAsync(() -> {
            try {
                stmt.executeQuery("SELECT 1");
            } catch (SQLException ignored) {
                // Expected: cancel or poll failure
            }
        });

        // Wait for the query to be submitted, then call cancel() from this thread
        assertThat(querySubmitted.await(5, TimeUnit.SECONDS))
                .as("Query should be submitted within 5 seconds")
                .isTrue();

        // Small delay to ensure the poll loop has started and currentQueryId is set
        Thread.sleep(100);

        stmt.cancel();

        assertThat(cancelDone.await(5, TimeUnit.SECONDS))
                .as("Cancel request should reach the server within 5 seconds")
                .isTrue();

        assertThat(cancelReceived.get())
                .as("Server should receive the cancel request")
                .isTrue();
        assertThat(cancelledQueryId.get()).isEqualTo(queryId);

        // Clean up: the query future will eventually fail or time out
        queryFuture.orTimeout(5, TimeUnit.SECONDS).exceptionally(e -> null).join();
    }

    @Test
    @DisplayName("cancel() is a no-op when no query is executing")
    void cancelIsNoOpWhenIdle() throws Exception {
        server.start();
        PangrovaStatement stmt = createStatement();

        // cancel() on an idle statement should not throw
        stmt.cancel();
        // No exception = success; no HTTP call made (server has no cancel context)
    }

    @Test
    @DisplayName("currentQueryId is cleared after query completes")
    void currentQueryIdClearedAfterCompletion() throws Exception {
        String queryId = "q-d41-clear-001";
        AtomicBoolean cancelReceived = new AtomicBoolean(false);

        // Submit endpoint: return FINISHED immediately with unreachable transport
        // (will cause fetchResult to fail, triggering the D42 cancel-on-disconnect)
        server.createContext("/v1/query/submit", exchange -> {
            String body = """
                {
                  "query_id": "%s",
                  "state": "FINISHED",
                  "result_endpoint": {
                    "host": "127.0.0.1",
                    "port": 1,
                    "ticket": "ticket-001",
                    "protocol": "grpc"
                  },
                  "column_names": ["col1"]
                }
                """.formatted(queryId);
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });

        // Cancel endpoint for D42 (cancel-on-disconnect)
        server.createContext("/v1/query/" + queryId + "/cancel", exchange -> {
            cancelReceived.set(true);
            String body = "{\"outcome\":\"CANCEL_ACCEPTED\"}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(202, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });

        server.start();

        PangrovaStatement stmt = createStatement();

        // Execute will fail (transport unreachable), D42 cancel fires, then currentQueryId is cleared
        try {
            stmt.executeQuery("SELECT 1");
        } catch (SQLException ignored) {
            // Expected: transport failure
        }

        // Reset the flag — the D42 cancel already fired
        cancelReceived.set(false);

        // Now call cancel() — should be a no-op since currentQueryId was cleared in finally
        stmt.cancel();

        assertThat(cancelReceived.get())
                .as("cancel() after query completion should not send another cancel request")
                .isFalse();
    }

    private PangrovaStatement createStatement() {
        PangrovaRestClient restClient = PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(port)
                .connectTimeout(Duration.ofSeconds(2))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
        PangrovaConnection conn = new PangrovaConnection(restClient, null);
        return new PangrovaStatement(conn);
    }
}
