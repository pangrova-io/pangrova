package org.pangrova.client.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.common.client.exception.PangrovaProtocolException;
import org.pangrova.common.enums.ResultDisposition;

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
                "{\"query_id\":\"q-stream\",\"state\":\"RUNNING\","
                        + "\"result_disposition\":\"PENDING\"}"));
        server.createContext("/v1/query/q-stream/state", exchange -> {
            String state = streamConsumed.get() ? "FINISHED" : "RUNNING";
            sendJson(exchange, """
                    {"query_id":"q-stream","state":"%s","version":%d,"drained":false,
                     "result_disposition":"STREAM",
                     "result_endpoint":{"host":"127.0.0.1","port":12345,
                                        "ticket":"q-stream","protocol":"grpc"}}
                    """.formatted(state, streamConsumed.get() ? 2 : 1));
        });
        server.start();

        try (PangrovaRestClient client = client()) {
            long deadline = QueryCompletionWaiter.deadlineFor(5);
            var query = client.beginQuery("SELECT 1", Map.of());
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(query, deadline, 5);
            assertThat(ready.terminal()).isFalse();
            assertThat(ready.disposition()).isEqualTo(ResultDisposition.STREAM);
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
                "{\"query_id\":\"q-failed\",\"state\":\"RUNNING\","
                        + "\"result_disposition\":\"PENDING\"}"));
        server.createContext("/v1/query/q-failed/state", exchange -> sendJson(exchange, """
                {"query_id":"q-failed","state":"FAILED","version":1,"drained":false,
                 "result_disposition":"STREAM",
                 "result_endpoint":{"host":"127.0.0.1","port":12345,
                                    "ticket":"q-failed","protocol":"grpc"}}
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
                 "result_disposition":"STREAM",
                 "result_endpoint":{"host":"127.0.0.1","port":12345,"ticket":"q-ready","protocol":"grpc"}}
                """));
        server.start();
        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("SELECT 1", Map.of());
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(query, QueryCompletionWaiter.deadlineFor(5), 5);
            assertThat(ready.endpoint()).isEqualTo(query.submission().resultEndpoint());
            assertThat(ready.terminal()).isFalse();
        }
    }

    @Test
    void finishedNoResultIsAValidTerminalOutcome() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-ddl\",\"state\":\"FINISHED\","
                        + "\"result_disposition\":\"NONE\"}"));
        server.start();

        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("CREATE TABLE t (id INT)", Map.of());
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(query, QueryCompletionWaiter.deadlineFor(5), 5);
            assertThat(ready.disposition()).isEqualTo(ResultDisposition.NONE);
            assertThat(ready.endpoint()).isNull();
            assertThat(ready.terminal()).isTrue();
        }
    }

    @Test
    void pendingStatusWaitsForDelayedEndpointPublication() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-delayed\",\"state\":\"RUNNING\","
                        + "\"result_disposition\":\"PENDING\"}"));
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        server.createContext("/v1/query/q-delayed/state", exchange -> {
            boolean published = reads.incrementAndGet() > 1;
            sendJson(exchange, published ? """
                    {"query_id":"q-delayed","state":"RUNNING","version":2,"drained":false,
                     "result_disposition":"STREAM",
                     "result_endpoint":{"host":"127.0.0.1","port":12345,
                                        "ticket":"delayed","protocol":"grpc"}}
                    """ : """
                    {"query_id":"q-delayed","state":"RUNNING","version":1,"drained":false,
                     "result_disposition":"PENDING"}
                    """);
        });
        server.start();

        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("SELECT 1", Map.of());
            var ready = QueryCompletionWaiter.awaitReadableEndpoint(query, QueryCompletionWaiter.deadlineFor(5), 5);
            assertThat(ready.disposition()).isEqualTo(ResultDisposition.STREAM);
            assertThat(ready.endpoint().ticket()).isEqualTo("delayed");
            assertThat(ready.terminal()).isFalse();
        }
    }

    @Test
    void cancellationBeforePublicationDoesNotWaitForAnEndpoint() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-cancelled\",\"state\":\"RUNNING\","
                        + "\"result_disposition\":\"PENDING\"}"));
        server.createContext("/v1/query/q-cancelled/state", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-cancelled\",\"state\":\"CANCELLED\",\"version\":2,"
                        + "\"result_disposition\":\"NONE\",\"drained\":false}"));
        server.start();

        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("SELECT 1", Map.of());
            assertThatThrownBy(() -> QueryCompletionWaiter.awaitReadableEndpoint(
                    query, QueryCompletionWaiter.deadlineFor(5), 5))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("cancelled");
        }
    }

    @Test
    void liveStatusRequiresDrainEvidence() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-no-drain\",\"state\":\"RUNNING\","
                        + "\"result_disposition\":\"PENDING\"}"));
        server.createContext("/v1/query/q-no-drain/state", exchange -> sendJson(exchange,
                "{\"query_id\":\"q-no-drain\",\"state\":\"RUNNING\",\"version\":1,"
                        + "\"result_disposition\":\"PENDING\"}"));
        server.start();
        try (PangrovaRestClient client = client()) {
            var query = client.beginQuery("SELECT 1", Map.of());
            assertThatThrownBy(() -> QueryCompletionWaiter.awaitReadableEndpoint(
                    query, QueryCompletionWaiter.deadlineFor(5), 5))
                    .isInstanceOf(PangrovaProtocolException.class);
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
