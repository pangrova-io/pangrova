package org.pangrova.client.http;

import org.pangrova.common.enums.QueryWaitTarget;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.client.session.ConnectionPoolConfig;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.JsonMapperFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class QueryOwnerRoutingTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void ownerReceiptCanDirectControlOnlyToAConfiguredEndpoint(boolean configured) throws Exception {
        var ingress = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var owner = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var unconfigured = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var inaccessibleCalls = new AtomicInteger();
        var ingressCalls = new CopyOnWriteArrayList<String>();
        var ownerCalls = new CopyOnWriteArrayList<String>();
        var ingressTickets = new CopyOnWriteArrayList<String>();
        var ownerTickets = new CopyOnWriteArrayList<String>();
        int port = (configured ? owner : unconfigured).getAddress().getPort();
        var submissions = new AtomicInteger();
        var endpoint = new QueryOwnerEndpoint("coord-original", "incarnation-original", "127.0.0.1", port,
                "fixture-route-ticket");
        var receipt = new SubmitQueryResponse("q", "QUEUED", ResultDisposition.PENDING,
                null, List.of(), null, endpoint);
        String body = JsonMapperFactory.writer().writeValueAsString(receipt);
        ingress.createContext("/v1/query/submit", exchange -> {
            submissions.incrementAndGet();
            reply(exchange, 200, body);
        });
        registerControlRoutes(ingress, ingressCalls, ingressTickets);
        registerControlRoutes(owner, ownerCalls, ownerTickets);
        unconfigured.createContext("/", exchange -> {
            inaccessibleCalls.incrementAndGet();
            reply(exchange, 404, "{}");
        });
        ingress.start();
        owner.start();
        unconfigured.start();
        var pool = ConnectionPoolConfig.withFailover(List.of(authority(ingress), authority(owner)));
        try {
            SubmitQueryResponse submitted;
            try (var client = PangrovaRestClient.builder().bearerToken("route-test-credential")
                    .connectionPoolConfig(pool).build()) {
                var query = client.beginQuery("SELECT 42", Map.of());
                submitted = query.submission();
                assertThat(submitted).isEqualTo(receipt);
                assertThat(query.awaitStatus(QueryWaitTarget.NEXT_VERSION, Long.MAX_VALUE).version()).isEqualTo(3L);
            }
            try (var client = PangrovaRestClient.builder().bearerToken("route-test-credential")
                    .connectionPoolConfig(pool).build()) {
                var query = client.resumeQuery(submitted);
                assertThat(query.awaitStatus(QueryWaitTarget.NEXT_VERSION, Long.MAX_VALUE).version()).isEqualTo(3L);
                assertThat(query.cancel()).isEqualTo(CancelOutcome.CANCEL_ACCEPTED);
            }
            assertThat(submissions.get()).isOne();
            assertThat(configured ? ingressCalls : ownerCalls).isEmpty();
            assertThat(configured ? ownerCalls : ingressCalls).containsExactly("state", "state", "cancel");
            if (configured) {
                assertThat(ownerTickets).containsExactly(null, null, null);
            } else {
                assertThat(ingressTickets).containsOnly(endpoint.routeTicket()).hasSize(3);
            }
            assertThat(inaccessibleCalls.get()).isZero();
        } finally {
            unconfigured.stop(0);
            owner.stop(0);
            ingress.stop(0);
        }
    }

    @Test
    void ownerConnectionLossRecoversTheSignedReceiptWithoutResubmitting() throws Exception {
        var owner = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var original = new QueryOwnerEndpoint("coord-original", "incarnation-original",
                "127.0.0.1", owner.getAddress().getPort(), "fixture-route-ticket");
        var receipt = new SubmitQueryResponse("q", "QUEUED", ResultDisposition.PENDING,
                null, List.of(), null, original);
        var receipts = new AtomicInteger();
        var unexpected = new CopyOnWriteArrayList<String>();
        var observed = new CopyOnWriteArrayList<String>();
        var tickets = new CopyOnWriteArrayList<String>();
        owner.start();
        var pool = ConnectionPoolConfig.withFailover(List.of(authority(owner), authority(peer)));
        owner.stop(0);

        peer.createContext("/v1/query/q/state", exchange -> {
            observed.add(exchange.getRequestURI().getRawQuery());
            tickets.add(exchange.getRequestHeaders().getFirst(QueryOwnerEndpoint.ROUTE_HEADER));
            reply(exchange, 503, """
                    {"error_code":"QUERY_OWNER_UNAVAILABLE","message":"Original owner is unavailable"}
                    """);
        });
        peer.createContext("/v1/query/q/receipt-recovery", exchange -> {
            tickets.add(exchange.getRequestHeaders().getFirst(QueryOwnerEndpoint.ROUTE_HEADER));
            if (receipts.incrementAndGet() == 1) {
                reply(exchange, 503, """
                        {"error_code":"QUERY_RECOVERY_PENDING","message":"Waiting for retirement"}
                        """);
            } else {
                reply(exchange, 200, """
                        {"query_id":"q","state":"FINISHED","result_disposition":"UNAVAILABLE","drained":true}
                        """);
            }
        });
        peer.createContext("/", exchange -> {
            unexpected.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            reply(exchange, 404, "{}");
        });
        peer.start();
        try (var client = PangrovaRestClient.builder().bearerToken("route-test-credential")
                .connectionPoolConfig(pool).requestTimeout(Duration.ofSeconds(2)).build()) {
            var query = client.resumeQuery(receipt);
            long deadline = System.currentTimeMillis() + 5_000;
            var terminal = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
            assertThat(terminal.recovered()).isTrue();
            assertThat(terminal.version()).isNull();
            assertThat(terminal.resultEndpoint()).isNull();
            assertThat(terminal.resultDisposition()).isEqualTo(ResultDisposition.UNAVAILABLE);
            assertThat(query.awaitStatus(QueryWaitTarget.TERMINAL, deadline)).isSameAs(terminal);
            assertThat(query.cancel()).isEqualTo(CancelOutcome.ALREADY_TERMINAL);
            assertThat(query.submission()).isEqualTo(receipt);
            assertThat(observed).containsExactly("afterVersion=0&waitMs=0&until=DRAINED");
            assertThat(tickets).containsOnly(original.routeTicket()).hasSize(3);
            assertThat(receipts.get()).isEqualTo(2);
            assertThat(unexpected).isEmpty();
        } finally {
            peer.stop(0);
        }
    }

    @Test
    void cancellationOfARecoveredQueryObservesRetirementWithoutReplayingThePost() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var original = new QueryOwnerEndpoint("coord", "old-incarnation", "127.0.0.1",
                server.getAddress().getPort(), "signed-receipt");
        var receipt = new SubmitQueryResponse("q", "QUEUED", ResultDisposition.PENDING,
                null, List.of(), null, original);
        var methods = new CopyOnWriteArrayList<String>();
        server.createContext("/v1/query/q/cancel", exchange -> {
            methods.add(exchange.getRequestMethod());
            reply(exchange, 404, """
                    {"query_id":"q","cancelled":false,"outcome":"NOT_FOUND"}
                    """);
        });
        server.createContext("/v1/query/q/receipt-recovery", exchange -> {
            methods.add(exchange.getRequestMethod());
            reply(exchange, 200, """
                    {"query_id":"q","state":"FINISHED","result_disposition":"NONE","drained":true}
                    """);
        });
        server.start();
        try (var client = PangrovaRestClient.builder().coordinatorHost(original.host())
                .coordinatorPort(original.port()).build()) {
            var query = client.resumeQuery(receipt);
            assertThat(query.cancel()).isEqualTo(CancelOutcome.ALREADY_TERMINAL);
            assertThat(query.awaitStatus(QueryWaitTarget.TERMINAL, Long.MAX_VALUE).resultDisposition())
                    .isEqualTo(ResultDisposition.NONE);
            assertThat(query.cancel()).isEqualTo(CancelOutcome.ALREADY_TERMINAL);
            assertThat(methods).containsExactly("POST", "GET");
        } finally {
            server.stop(0);
        }
    }

    private static void registerControlRoutes(HttpServer server, List<String> calls, List<String> tickets) {
        server.createContext("/v1/query/q/state", exchange -> {
            calls.add("state");
            tickets.add(exchange.getRequestHeaders().getFirst(QueryOwnerEndpoint.ROUTE_HEADER));
            reply(exchange, 200, """
                    {"query_id":"q","state":"RUNNING","result_disposition":"PENDING","version":3,"drained":false}
                    """);
        });
        server.createContext("/v1/query/q/cancel", exchange -> {
            calls.add("cancel");
            tickets.add(exchange.getRequestHeaders().getFirst(QueryOwnerEndpoint.ROUTE_HEADER));
            reply(exchange, 202, """
                    {"query_id":"q","cancelled":true,"outcome":"CANCEL_ACCEPTED"}
                    """);
        });
    }

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
