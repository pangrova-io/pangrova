package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryCompletionWaiterTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void publishedEndpointIsReadableBeforeNativeOutputFinishes() throws Exception {
        AtomicBoolean streamConsumed = new AtomicBoolean(false);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-stream\",\"state\":\"RUNNING\"}"));
        server.createContext("/v1/query/q-stream/state", exchange -> {
            String state = streamConsumed.get() ? "FINISHED" : "RUNNING";
            sendJson(exchange, """
                    {"query_id":"q-stream","state":"%s","version":%d,
                     "result_endpoint":{"host":"127.0.0.1","port":12345,
                                        "ticket":"q-stream","protocol":"flight"}}
                    """.formatted(state, streamConsumed.get() ? 2 : 1));
        });
        server.start();

        try (PangrovaRestClient client = client()) {
            long deadline = QueryCompletionWaiter.deadlineFor(5);
            var query = client.beginQuery("SELECT 1", Map.of());
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(query, deadline, 5);
            assertThat(ready.terminal()).isFalse();
            assertThat(ready.endpoint().ticket()).isEqualTo("q-stream");

            // DoGet consumes the stream while the query is still RUNNING.
            streamConsumed.set(true);
            QueryCompletionWaiter.awaitTerminalSuccess(query, deadline, 5);
        }
    }

    @Test
    void terminalFailureTakesPrecedenceOverPublishedEndpoint() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-failed\",\"state\":\"RUNNING\"}"));
        server.createContext("/v1/query/q-failed/state", exchange -> sendJson(exchange, """
                {"query_id":"q-failed","state":"FAILED","version":1,
                 "result_endpoint":{"host":"127.0.0.1","port":12345,
                                    "ticket":"q-failed","protocol":"flight"}}
                """));
        server.start();

        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("SELECT 1", Map.of());
            assertThatThrownBy(() -> QueryCompletionWaiter.awaitReadableEndpoint(
                    query, QueryCompletionWaiter.deadlineFor(5), 5))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Query failed");
        }
    }

    @Test
    void runningSubmitResponseCanExposeReadyEndpointImmediately() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange, """
                {"query_id":"q-ready","state":"RUNNING",
                 "result_endpoint":{"host":"127.0.0.1","port":12345,"ticket":"q-ready","protocol":"flight"}}
                """));
        server.start();
        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("SELECT 1", Map.of());
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(query, QueryCompletionWaiter.deadlineFor(5), 5);
            assertThat(ready.endpoint()).isEqualTo(query.submission().resultEndpoint());
            assertThat(ready.terminal()).isFalse();
        }
    }

    private PangrovaRestClient client() {
        return PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(server.getAddress().getPort())
                .connectTimeout(Duration.ofSeconds(2))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
    }

    private static void sendJson(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
