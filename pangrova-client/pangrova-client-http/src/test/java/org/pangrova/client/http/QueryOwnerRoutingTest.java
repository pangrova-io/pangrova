package org.pangrova.client.http;

import org.pangrova.common.enums.QueryWaitTarget;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.session.ConnectionPoolConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class QueryOwnerRoutingTest {
    @Test
    void unrelatedFailoverCannotMoveVersionedWaitOrCancellationToAnotherCoordinator() throws Exception {
        var owner = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var ownerQueries = new CopyOnWriteArrayList<String>();
        var observerIds = new CopyOnWriteArrayList<String>();
        var peerPaths = new CopyOnWriteArrayList<String>();
        owner.createContext("/v1/query/submit", exchange -> reply(exchange, 200,
                "{\"query_id\":\"owner-query\",\"state\":\"RUNNING\","
                        + "\"result_disposition\":\"PENDING\"}"));
        owner.createContext("/v1/query/owner-query/state", exchange -> {
            ownerQueries.add(exchange.getRequestURI().getRawQuery());
            observerIds.add(exchange.getRequestHeaders().getFirst("X-Pangrova-Query-Observer"));
            reply(exchange, 200, """
                    {"query_id":"owner-query","state":"RUNNING","result_disposition":"PENDING",
                     "version":%d,"drained":false}
                    """.formatted(10 + ownerQueries.size()));
        });
        owner.createContext("/v1/query/owner-query/cancel", exchange -> reply(exchange, 202, """
                {"query_id":"owner-query","cancelled":true,"outcome":"CANCEL_ACCEPTED"}
                """));
        owner.createContext("/v1/query/diagnostic/state", exchange -> reply(exchange, 503, "{}"));
        peer.createContext("/", exchange -> {
            peerPaths.add(exchange.getRequestURI().getPath());
            reply(exchange, 200, "{\"query_id\":\"diagnostic\",\"state\":\"FINISHED\","
                    + "\"result_disposition\":\"NONE\"}");
        });
        owner.start();
        peer.start();
        try (var client = PangrovaRestClient.builder().connectionPoolConfig(ConnectionPoolConfig.withFailover(
                List.of(authority(owner), authority(peer)))).build()) {
            var query = client.beginQuery("SELECT 42", Map.of());
            assertThat(query.submission().queryId()).isEqualTo("owner-query");
            assertThat(query.awaitStatus(QueryWaitTarget.NEXT_VERSION, Long.MAX_VALUE).version()).isEqualTo(11L);
            assertThat(client.getQueryStatus("diagnostic").state()).isEqualTo("FINISHED");
            assertThat(query.awaitStatus(QueryWaitTarget.NEXT_VERSION, Long.MAX_VALUE).version()).isEqualTo(12L);
            assertThat(query.cancel()).isEqualTo(CancelOutcome.CANCEL_ACCEPTED);
            assertThat(ownerQueries).containsExactly("afterVersion=0&waitMs=0", "afterVersion=11&waitMs=5000");
            assertThat(observerIds).hasSize(2).containsOnly(observerIds.get(0));
            assertThat(peerPaths).containsExactly("/v1/query/diagnostic/state");
        } finally {
            peer.stop(0);
            owner.stop(0);
        }
    }

    private static String authority(HttpServer server) {
        return "127.0.0.1:" + server.getAddress().getPort();
    }

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        try (exchange) {
            exchange.getRequestBody().readAllBytes();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }
}
