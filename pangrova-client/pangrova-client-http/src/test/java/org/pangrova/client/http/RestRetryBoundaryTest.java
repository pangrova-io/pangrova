package org.pangrova.client.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.exception.PangrovaServerException;
import org.pangrova.common.client.session.RetryPolicy;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/** Real HTTP retry/timer boundaries; not Coordinator or SQL execution coverage. */
class RestRetryBoundaryTest {
    @Test
    void largestRetryCountStillSendsItsInitialActualHttpRequest() throws Exception {
        try (var peer = new StatusPeer();
             var client = peer.client(new RetryPolicy(Integer.MAX_VALUE, Duration.ZERO, Duration.ZERO, 1.0),
                     Duration.ofSeconds(2), Duration.ofSeconds(2))) {
            var status = client.getQueryStatus("boundary-query");
            assertThat(status.queryId()).isEqualTo("boundary-query");
            assertThat(status.state()).isEqualTo("RUNNING");
            assertThat(peer.requests.get()).isEqualTo(1);
        }
    }

    @Test
    void actualHttpTimersDoNotRequireDurationToFitSignedLongNanoseconds() throws Exception {
        Duration timeout = Duration.ofNanos(Long.MAX_VALUE).plusNanos(1);
        assertThatThrownBy(timeout::toNanos).isInstanceOf(ArithmeticException.class);
        try (var peer = new StatusPeer();
             var client = peer.client(RetryPolicy.NONE, timeout, timeout)) {
            var status = client.getQueryStatus("boundary-query");
            assertThat(status.queryId()).isEqualTo("boundary-query");
            assertThat(status.state()).isEqualTo("RUNNING");
            assertThat(peer.requests.get()).isEqualTo(1);
        }
    }

    @Test
    void nonIdempotentSubmissionIsNotReplayedAfterAnActualServerError() throws Exception {
        try (var peer = new StatusPeer(1);
             var client = peer.client(new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0),
                     Duration.ofSeconds(2), Duration.ofSeconds(2))) {
            String sql = "SELECT 'retry payload' AS value";
            assertThatThrownBy(() -> client.submitQuery(sql, Map.of()))
                    .isInstanceOf(PangrovaServerException.class)
                    .satisfies(failure -> assertThat(((PangrovaServerException) failure).httpStatusCode())
                            .isEqualTo(503));
            assertThat(peer.requests.get()).isEqualTo(1);
            List<String> bodies = List.copyOf(peer.requestBodies);
            assertThat(bodies).hasSize(1);
            assertThat(bodies.get(0)).contains(sql);
            var request = JsonMapperFactory.create().readValue(bodies.get(0), SubmitQueryRequest.class);
            assertThat(request.query()).isEqualTo(sql);
            assertThat(peer.methods).containsExactly("POST");
            assertThat(peer.paths).containsExactly("/v1/query/submit");
        }
    }

    @Test
    void idempotentStatusRetryPreservesActualGetTargetAndFinalResponse() throws Exception {
        try (var peer = new StatusPeer(1);
             var client = peer.client(new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0),
                     Duration.ofSeconds(2), Duration.ofSeconds(2))) {
            var response = client.getQueryStatus("boundary-query");
            assertThat(response.queryId()).isEqualTo("boundary-query");
            assertThat(response.state()).isEqualTo("RUNNING");
            assertThat(peer.requests.get()).isEqualTo(2);
            assertThat(peer.methods).containsExactly("GET", "GET");
            assertThat(peer.paths).containsExactly("/v1/query/boundary-query/state",
                    "/v1/query/boundary-query/state");
            assertThat(peer.requestBodies).containsExactly("", "");
        }
    }

    @Test
    void exactSupportedTimerMaximumIsRetainedByBothConfigGetters() {
        Duration maximum = Duration.ofMillis(Long.MAX_VALUE / 2);
        var config = new PangrovaRestClientConfig("localhost", 8080, maximum, maximum);
        assertThat(config.connectTimeout()).isEqualTo(maximum);
        assertThat(config.requestTimeout()).isEqualTo(maximum);
    }

    @Test
    void evenOneNanosecondBeyondSupportedTimerMaximumIsRejectedForEachField() {
        Duration overflow = Duration.ofMillis(Long.MAX_VALUE / 2).plusNanos(1);
        assertAll(
                () -> assertTimeoutRejected(true, overflow),
                () -> assertTimeoutRejected(false, overflow));
    }

    @Test
    void connectTimeoutRejectsOverflowBeforeAllocatingAnHttpOwner() {
        assertAll(
                () -> assertTimeoutRejected(true, Duration.ofMillis(Long.MAX_VALUE).plusMillis(1)),
                () -> assertTimeoutRejected(true, Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    void requestTimeoutRejectsOverflowBeforeAllocatingAnHttpOwner() {
        assertAll(
                () -> assertTimeoutRejected(false, Duration.ofMillis(Long.MAX_VALUE).plusMillis(1)),
                () -> assertTimeoutRejected(false, Duration.ofSeconds(Long.MAX_VALUE)));
    }

    private static void assertTimeoutRejected(boolean connect, Duration overflow) {
        Duration normal = Duration.ofSeconds(2);
        assertThatThrownBy(() -> new PangrovaRestClientConfig("localhost", 8080,
                connect ? overflow : normal, connect ? normal : overflow))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(connect ? "connectTimeout" : "requestTimeout");
    }

    private static final class StatusPeer implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor;
        private final AtomicInteger requests = new AtomicInteger();
        private final ConcurrentLinkedQueue<String> requestBodies = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<String> methods = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<String> paths = new ConcurrentLinkedQueue<>();

        private StatusPeer() throws Exception {
            this(0);
        }

        private StatusPeer(int retryableResponses) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = ManagedExecutorFactory.newFixedThreadPool("rest-retry-boundary-http", 1);
            try {
                server.setExecutor(executor);
                server.createContext("/v1/query/boundary-query/state",
                        exchange -> serve(exchange, retryableResponses));
                server.createContext("/v1/query/submit", exchange -> serve(exchange, retryableResponses));
                server.start();
            } catch (Exception | Error failure) {
                try {
                    close();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        private void serve(HttpExchange exchange, int retryableResponses) throws java.io.IOException {
            try (exchange) {
                requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                methods.add(exchange.getRequestMethod());
                paths.add(exchange.getRequestURI().getRawPath());
                boolean retry = requests.incrementAndGet() <= retryableResponses;
                String payload = retry
                        ? "{\"error_code\":\"UNAVAILABLE\",\"message\":\"retry this actual HTTP request\"}"
                        : "{\"query_id\":\"boundary-query\",\"state\":\"RUNNING\"}";
                byte[] body = payload.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(retry ? 503 : 200, body.length);
                exchange.getResponseBody().write(body);
            }
        }

        private PangrovaRestClient client(RetryPolicy policy, Duration connect, Duration request) {
            return PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                    .coordinatorPort(server.getAddress().getPort()).httpExecutorThreads(1)
                    .retryPolicy(policy).connectTimeout(connect).requestTimeout(request).build();
        }

        @Override
        public void close() throws Exception {
            server.stop(0);
            ExecutorDrain.shutdownAndAwait(executor, 5_000);
            assertThat(executor.isTerminated()).isTrue();
        }
    }
}
