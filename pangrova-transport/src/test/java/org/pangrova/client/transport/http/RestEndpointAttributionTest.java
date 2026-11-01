package org.pangrova.client.transport.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.client.protocol.QueryStatusResponse;
import org.pangrova.client.protocol.SubmitQueryRequest;
import org.pangrova.client.error.PangrovaServerException;
import org.pangrova.client.session.ConnectionPoolConfig;
import org.pangrova.client.session.RetryPolicy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual HTTP routing interleaving and guards; not Coordinator or SQL execution. */
class RestEndpointAttributionTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static final RetryPolicy RETRIES = new RetryPolicy(2, Duration.ZERO, Duration.ZERO, 1.0);

    @Test
    void requestBuiltBeforeAnotherFailoverMustNotSkipTheHealthyNextEndpoint() throws Exception {
        try (var peers = new RoutingPeers(); var owner = RestHttpClientOwner.create(peers.config(true), null)) {
            var routing = new EndpointRouting(peers.endpoints(), "http");
            var executor = new RetryableHttpExecutor(owner, RETRIES, routing, TIMEOUT);
            // This is the real request-build/executor boundary, deliberately interleaved;
            // the routing state is advanced only by the other actual HTTP operation.
            var retained = peers.statusRequest(routing, "retained-query");
            var advance = executor.executeWithRetry(peers.statusRequest(routing, "advancing-query"));
            assertThat(advance.statusCode()).isEqualTo(200);
            assertThat(peers.sequence("/v1/query/advancing-query/state")).containsExactly("A", "B");
            assertThat(routing.current().index()).isEqualTo(1);

            var response = executor.executeWithRetry(retained);

            assertThat(response.statusCode()).isEqualTo(200);
            var status = JsonMapperFactory.create().readValue(response.body(), QueryStatusResponse.class);
            assertThat(status.queryId()).isEqualTo("retained-query");
            assertThat(status.state()).isEqualTo("RUNNING");
            assertThat(peers.sequence("/v1/query/retained-query/state")).containsExactly("A", "B");
        }
    }

    @Test
    void actualRepeatedRingWrapCannotLetAnOldGenerationAdvanceTheCurrentPreference() throws Exception {
        try (var peers = new RoutingPeers(true, 0);
             var owner = RestHttpClientOwner.create(peers.config(true), null)) {
            var routing = new EndpointRouting(peers.endpoints(), "http");
            var executor = new RetryableHttpExecutor(owner, RETRIES, routing, TIMEOUT);
            var retained = peers.statusRequest(routing, "old-generation");
            for (int round = 0; round < 3; round++) {
                String queryId = "wrap-" + round;
                var response = executor.executeWithRetry(peers.statusRequest(routing, queryId));
                assertThat(response.statusCode()).isEqualTo(503);
                assertThat(peers.sequence("/v1/query/" + queryId + "/state")).containsExactly("A", "B", "C");
                assertThat(routing.current().index()).isZero();
                assertThat(routing.current()).isNotSameAs(retained.selection());
            }
            var currentGeneration = routing.current();

            assertThat(executor.executeWithRetry(retained).statusCode()).isEqualTo(503);

            assertThat(peers.sequence("/v1/query/old-generation/state")).containsExactly("A", "B", "C");
            assertThat(routing.current()).isSameAs(currentGeneration);
            var fresh = executor.executeWithRetry(peers.statusRequest(routing, "fresh-generation"));
            assertThat(fresh.statusCode()).isEqualTo(503);
            assertThat(peers.sequence("/v1/query/fresh-generation/state")).containsExactly("A", "B", "C");
            assertThat(routing.current().index()).isZero();
            assertThat(routing.current()).isNotSameAs(currentGeneration);
        }
    }

    @Test
    void simultaneousFailuresOfTheSameSelectionNeverSkipTheHealthyNextEndpoint() throws Exception {
        int count = 4;
        try (var peers = new RoutingPeers(false, count);
             var owner = RestHttpClientOwner.create(peers.config(true), null)) {
            var routing = new EndpointRouting(peers.endpoints(), "http");
            var executor = new RetryableHttpExecutor(owner, RETRIES, routing, TIMEOUT);
            var requests = new ArrayList<EndpointRouting.SelectedRequest>();
            for (int index = 0; index < count; index++) {
                requests.add(peers.statusRequest(routing, "same-selection-" + index));
            }
            assertThat(requests).allSatisfy(request ->
                    assertThat(request.selection()).isSameAs(requests.get(0).selection()));
            ExecutorService callers = ManagedExecutorFactory.newFixedThreadPool("rest-routing-callers", count);
            try {
                var results = new ArrayList<Future<java.net.http.HttpResponse<String>>>();
                for (var request : requests) {
                    results.add(callers.submit(() -> executor.executeWithRetry(request)));
                }
                assertThat(peers.aEntered.await(5, TimeUnit.SECONDS)).isTrue();
                peers.aRelease.countDown();
                for (int index = 0; index < count; index++) {
                    var response = results.get(index).get(5, TimeUnit.SECONDS);
                    assertThat(response.statusCode()).isEqualTo(200);
                    String queryId = "same-selection-" + index;
                    var status = JsonMapperFactory.create().readValue(response.body(), QueryStatusResponse.class);
                    assertThat(status.queryId()).isEqualTo(queryId);
                    assertThat(status.state()).isEqualTo("RUNNING");
                    assertThat(peers.sequence("/v1/query/" + queryId + "/state")).containsExactly("A", "B");
                }
                assertThat(routing.current().index()).isEqualTo(1);
                var next = executor.executeWithRetry(peers.statusRequest(routing, "after-concurrent"));
                assertThat(next.statusCode()).isEqualTo(200);
                assertThat(peers.sequence("/v1/query/after-concurrent/state")).containsExactly("B");
            } finally {
                peers.aRelease.countDown();
                ExecutorDrain.shutdownAndAwait(callers, 5_000);
            }
            assertThat(callers.isTerminated()).isTrue();
        }
    }

    @Test
    void disabledFailoverRetriesOnlyTheConfiguredCoordinatorDespiteHealthyAlternates() throws Exception {
        try (var peers = new RoutingPeers(); var client = peers.client(false)) {
            assertThatThrownBy(() -> client.getQueryStatus("no-failover-query"))
                    .isInstanceOf(PangrovaServerException.class)
                    .satisfies(failure -> assertThat(((PangrovaServerException) failure).httpStatusCode())
                            .isEqualTo(503));
            assertThat(peers.sequence("/v1/query/no-failover-query/state")).containsExactly("A", "A", "A");
            assertThat(peers.wires).hasSize(3).allSatisfy(wire -> {
                assertThat(wire.method()).isEqualTo("GET");
                assertThat(wire.body()).isEmpty();
            });
        }
    }

    @Test
    void nonIdempotentSqlSubmissionDoesNotReplayOrFailOverAfterActual503() throws Exception {
        try (var peers = new RoutingPeers(); var client = peers.client(true)) {
            String sql = "INSERT INTO t VALUES ('exact mutation payload')";
            assertThatThrownBy(() -> client.submitQuery(sql, Map.of()))
                    .isInstanceOf(PangrovaServerException.class)
                    .satisfies(failure -> assertThat(((PangrovaServerException) failure).httpStatusCode())
                            .isEqualTo(503));
            assertThat(peers.sequence("/v1/query/submit")).containsExactly("A");
            assertThat(peers.wires).hasSize(1);
            Wire wire = List.copyOf(peers.wires).get(0);
            assertThat(wire.method()).isEqualTo("POST");
            var request = JsonMapperFactory.create().readValue(wire.body(), SubmitQueryRequest.class);
            assertThat(request.query()).isEqualTo(sql);
        }
    }

    private record Wire(String endpoint, String method, String path, String body) {}

    private static final class RoutingPeers implements AutoCloseable {
        private final List<HttpServer> servers = new ArrayList<>();
        private final ExecutorService executor =
                ManagedExecutorFactory.newFixedThreadPool("rest-routing-peer-http", 4);
        private final ConcurrentLinkedQueue<Wire> wires = new ConcurrentLinkedQueue<>();
        private final boolean allUnavailable;
        private final CountDownLatch aEntered;
        private final CountDownLatch aRelease;

        private RoutingPeers() throws Exception {
            this(false, 0);
        }

        private RoutingPeers(boolean allUnavailable, int heldARequests) throws Exception {
            this.allUnavailable = allUnavailable;
            aEntered = new CountDownLatch(heldARequests);
            aRelease = new CountDownLatch(heldARequests == 0 ? 0 : 1);
            try {
                for (String endpoint : List.of("A", "B", "C")) {
                    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                    servers.add(server);
                    server.setExecutor(executor);
                    server.createContext("/v1/query/", exchange -> serve(exchange, endpoint));
                    server.start();
                }
            } catch (Exception | Error failure) {
                try {
                    close();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        private void serve(HttpExchange exchange, String endpoint) throws IOException {
            try (exchange) {
                String path = exchange.getRequestURI().getRawPath();
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                wires.add(new Wire(endpoint, exchange.getRequestMethod(), path, body));
                if (endpoint.equals("A")) {
                    aEntered.countDown();
                    awaitARelease();
                }
                boolean unavailable = allUnavailable || endpoint.equals("A");
                String queryId = exchange.getRequestURI().getPath().split("/")[3];
                String payload = unavailable
                        ? "{\"error_code\":\"UNAVAILABLE\",\"message\":\"actual endpoint unavailable\"}"
                        : "{\"query_id\":\"%s\",\"state\":\"RUNNING\"}".formatted(queryId);
                byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(unavailable ? 503 : 200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        }

        private void awaitARelease() throws IOException {
            try {
                if (!aRelease.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("Actual endpoint response barrier did not release");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("Actual endpoint response barrier interrupted", interrupted);
            }
        }

        private List<String> endpoints() {
            return servers.stream().map(server -> "127.0.0.1:" + server.getAddress().getPort()).toList();
        }

        private ConnectionPoolConfig pool(boolean failover) {
            return new ConnectionPoolConfig(endpoints(), Duration.ofSeconds(30), Duration.ofMinutes(5), 1, failover);
        }

        private PangrovaRestClientConfig config(boolean failover) {
            return new PangrovaRestClientConfig("127.0.0.1", servers.get(0).getAddress().getPort(),
                    TIMEOUT, TIMEOUT, false, null, null, null, RETRIES, pool(failover), 1, null);
        }

        private PangrovaRestClient client(boolean failover) {
            return PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                    .coordinatorPort(servers.get(0).getAddress().getPort()).connectTimeout(TIMEOUT)
                    .requestTimeout(TIMEOUT).retryPolicy(RETRIES).connectionPoolConfig(pool(failover))
                    .httpExecutorThreads(1).build();
        }

        private EndpointRouting.SelectedRequest statusRequest(EndpointRouting routing, String queryId) {
            var selection = routing.current();
            var request = HttpRequest.newBuilder(URI.create(selection.baseUrl()
                    + "/v1/query/" + queryId + "/state")).timeout(TIMEOUT).GET().build();
            return routing.bind(selection, request);
        }

        private List<String> sequence(String path) {
            return wires.stream().filter(wire -> wire.path().equals(path)).map(Wire::endpoint).toList();
        }

        @Override
        public void close() throws Exception {
            aRelease.countDown();
            RuntimeException failure = null;
            for (var server : servers) {
                try {
                    server.stop(0);
                } catch (RuntimeException cleanup) {
                    if (failure == null) {
                        failure = cleanup;
                    } else {
                        failure.addSuppressed(cleanup);
                    }
                }
            }
            try {
                ExecutorDrain.shutdownAndAwait(executor, 5_000);
            } catch (Exception cleanup) {
                if (failure != null) {
                    failure.addSuppressed(cleanup);
                } else {
                    throw cleanup;
                }
            }
            if (failure != null) {
                throw failure;
            }
            assertThat(executor.isTerminated()).isTrue();
        }
    }
}
