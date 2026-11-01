package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.client.transport.http.PangrovaRestClient;
import org.pangrova.client.protocol.ResultEndpoint;
import org.pangrova.client.protocol.SubmitQueryResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
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
        server.createContext("/v1/query/q-stream/state", exchange -> {
            String state = streamConsumed.get() ? "FINISHED" : "RUNNING";
            sendJson(exchange, """
                    {"query_id":"q-stream","state":"%s",
                     "result_endpoint":{"host":"127.0.0.1","port":12345,
                                        "ticket":"q-stream","protocol":"flight"}}
                    """.formatted(state));
        });
        server.start();

        try (PangrovaRestClient client = client()) {
            long deadline = QueryCompletionWaiter.deadlineFor(5);
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(
                    client, "q-stream",
                    new SubmitQueryResponse("q-stream", "RUNNING", null, List.of()),
                    deadline, 5);
            assertThat(ready.terminal()).isFalse();
            assertThat(ready.endpoint().ticket()).isEqualTo("q-stream");

            // DoGet consumes the stream while the query is still RUNNING.
            streamConsumed.set(true);
            QueryCompletionWaiter.awaitTerminalSuccess(client, "q-stream", deadline, 5);
        }
    }

    @Test
    void terminalFailureTakesPrecedenceOverPublishedEndpoint() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/q-failed/state", exchange -> sendJson(exchange, """
                {"query_id":"q-failed","state":"FAILED",
                 "result_endpoint":{"host":"127.0.0.1","port":12345,
                                    "ticket":"q-failed","protocol":"flight"}}
                """));
        server.start();

        try (PangrovaRestClient client = client()) {
            assertThatThrownBy(() -> QueryCompletionWaiter.awaitReadableEndpoint(
                    client, "q-failed",
                    new SubmitQueryResponse("q-failed", "RUNNING", null, List.of()),
                    QueryCompletionWaiter.deadlineFor(5), 5))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Query failed");
        }
    }

    @Test
    void runningSubmitResponseCanExposeReadyEndpointImmediately() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
        ResultEndpoint endpoint = new ResultEndpoint("127.0.0.1", 12345, "q-ready", "flight");
        try (PangrovaRestClient client = client()) {
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(
                    client, "q-ready",
                    new SubmitQueryResponse("q-ready", "RUNNING", endpoint, List.of()),
                    QueryCompletionWaiter.deadlineFor(5), 5);
            assertThat(ready.endpoint()).isEqualTo(endpoint);
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
