package org.pangrova.client.transport.http;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.client.core.error.PangrovaConnectionException;
import org.pangrova.client.core.session.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies spec task 184: the non-idempotent {@code /v1/query/submit} POST must not be
 * retried after the request bytes may have been sent (REQUEST_TIMEOUT / CONNECTION_FAILED),
 * since a server that already created the query would otherwise receive duplicate submits.
 *
 * <p>Also verifies that a client-side idempotency key is attached to the submit request so a
 * future coordinator can dedup, and that the generic (idempotent) GET path keeps retrying.
 */
@DisplayName("Submit idempotency / no-retry-after-send")
class PangrovaRestClientSubmitIdempotencyTest {

    // Aggressive timeout + fast backoff so post-send retries (if any) happen quickly.
    private static final RetryPolicy FAST_RETRY =
            new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(50), 2.0);
    private static final Duration SHORT_REQUEST_TIMEOUT = Duration.ofMillis(300);

    private HttpServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        port = server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private PangrovaRestClient client() {
        return PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1")
                .coordinatorPort(port)
                .requestTimeout(SHORT_REQUEST_TIMEOUT)
                .retryPolicy(FAST_RETRY)
                .build();
    }

    @Test
    @DisplayName("submit is NOT re-sent after a response timeout (request may have reached server)")
    void submitDoesNotRetryAfterRequestTimeout() {
        AtomicInteger submitInvocations = new AtomicInteger();
        server.createContext("/v1/query/submit", exchange -> {
            submitInvocations.incrementAndGet();
            try {
                // Sleep longer than the client's request timeout to force REQUEST_TIMEOUT
                // AFTER the request body has been received by the server.
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                byte[] body = "{\"query_id\":\"q-1\",\"state\":\"QUEUED\"}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (IOException ignored) {
                // client already disconnected
            } finally {
                exchange.close();
            }
        });

        try (PangrovaRestClient client = client()) {
            assertThatThrownBy(() -> client.submitQuery("SELECT 1", Map.of()))
                    .isInstanceOf(PangrovaConnectionException.class);
        }

        // Server received the submit exactly once; the client must NOT have re-POSTed it.
        assertThat(submitInvocations.get())
                .as("submit must be attempted exactly once after a post-send timeout")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("submit carries a non-blank Idempotency-Key / X-Pangrova-Request-Id header")
    void submitSendsIdempotencyKeyHeader() {
        AtomicReference<String> idempotencyKey = new AtomicReference<>();
        AtomicReference<String> requestId = new AtomicReference<>();
        server.createContext("/v1/query/submit", exchange -> {
            idempotencyKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            requestId.set(exchange.getRequestHeaders().getFirst("X-Pangrova-Request-Id"));
            exchange.getRequestBody().readAllBytes();
            byte[] body = "{\"query_id\":\"q-1\",\"state\":\"QUEUED\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        try (PangrovaRestClient client = client()) {
            var resp = client.submitQuery("SELECT 1", Map.of());
            assertThat(resp.queryId()).isEqualTo("q-1");
        }

        assertThat(idempotencyKey.get())
                .as("Idempotency-Key header must be present on submit")
                .isNotBlank();
        assertThat(requestId.get())
                .as("X-Pangrova-Request-Id header must be present on submit")
                .isNotBlank();
    }

    @Test
    @DisplayName("idempotent GET (query status) still retries after a timeout")
    void getStatusStillRetriesAfterTimeout() {
        AtomicInteger statusInvocations = new AtomicInteger();
        server.createContext("/v1/query/q-1/state", exchange -> {
            statusInvocations.incrementAndGet();
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (IOException ignored) {
                // client already disconnected
            } finally {
                exchange.close();
            }
        });

        try (PangrovaRestClient client = client()) {
            assertThatThrownBy(() -> client.getQueryStatus("q-1"))
                    .isInstanceOf(PangrovaConnectionException.class);
        }

        // Generic GET path is unchanged: maxRetries(3) + 1 = 4 attempts.
        assertThat(statusInvocations.get())
                .as("idempotent GET should keep retrying (4 attempts)")
                .isEqualTo(FAST_RETRY.maxRetries() + 1);
    }
}
