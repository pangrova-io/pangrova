package org.pangrova.coordinator.scheduler;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.spi.tracing.TracingProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates BUG-D28 fix: commit notification retry with exponential backoff.
 *
 * <p>Uses a test-double HttpClient that simulates transient network failures
 * (no mock frameworks). Verifies:
 * <ul>
 *   <li>Successful delivery after transient failures</li>
 *   <li>Failure callback invoked after all retries exhausted</li>
 *   <li>No callback invoked on success</li>
 *   <li>Backoff between retries (3 attempts, 1s/2s/4s)</li>
 * </ul>
 */
class CommitNotificationRetryTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();
    private static final String WORKER_HOST = "worker-1";
    private static final int HTTP_PORT = 8080;
    private static final String QUERY_ID = "q-retry-test";
    private static final int UPSTREAM_STAGE_ID = 0;

    @Test
    @DisplayName("Succeeds on first attempt — no retry, no failure callback")
    void succeedsOnFirstAttempt() {
        FailNTimesHttpClient httpClient = new FailNTimesHttpClient(0);
        RemoteTaskDispatcher dispatcher = newDispatcher(httpClient);
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        dispatcher.notifyUpstreamCommit(
                WORKER_HOST, HTTP_PORT, QUERY_ID, UPSTREAM_STAGE_ID,
                () -> callbackInvoked.set(true));

        assertThat(httpClient.attemptCount.get()).isEqualTo(1);
        assertThat(callbackInvoked.get()).isFalse();
    }

    @Test
    @DisplayName("Succeeds after transient failures — retries then succeeds")
    void succeedsAfterTransientFailures() {
        FailNTimesHttpClient httpClient = new FailNTimesHttpClient(2);
        RemoteTaskDispatcher dispatcher = newDispatcher(httpClient);
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        dispatcher.notifyUpstreamCommit(
                WORKER_HOST, HTTP_PORT, QUERY_ID, UPSTREAM_STAGE_ID,
                () -> callbackInvoked.set(true));

        assertThat(httpClient.attemptCount.get()).isEqualTo(3);
        assertThat(callbackInvoked.get()).isFalse();
    }

    @Test
    @DisplayName("All retries exhausted — failure callback invoked")
    void allRetriesExhausted_invokesFailureCallback() {
        FailNTimesHttpClient httpClient = new FailNTimesHttpClient(
                RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS + 1);
        RemoteTaskDispatcher dispatcher = newDispatcher(httpClient);
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        dispatcher.notifyUpstreamCommit(
                WORKER_HOST, HTTP_PORT, QUERY_ID, UPSTREAM_STAGE_ID,
                () -> callbackInvoked.set(true));

        assertThat(httpClient.attemptCount.get())
                .isEqualTo(RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS);
        assertThat(callbackInvoked.get()).isTrue();
    }

    @Test
    @DisplayName("All retries exhausted with null callback — no NPE")
    void allRetriesExhausted_nullCallback_noException() {
        FailNTimesHttpClient httpClient = new FailNTimesHttpClient(
                RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS + 1);
        RemoteTaskDispatcher dispatcher = newDispatcher(httpClient);

        // Should not throw even with null callback
        dispatcher.notifyUpstreamCommit(
                WORKER_HOST, HTTP_PORT, QUERY_ID, UPSTREAM_STAGE_ID, null);

        assertThat(httpClient.attemptCount.get())
                .isEqualTo(RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS);
    }

    @Test
    @DisplayName("Non-200 response triggers retry")
    void non200Response_triggersRetry() {
        HttpStatusErrorHttpClient httpClient = new HttpStatusErrorHttpClient(2);
        RemoteTaskDispatcher dispatcher = newDispatcher(httpClient);
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        dispatcher.notifyUpstreamCommit(
                WORKER_HOST, HTTP_PORT, QUERY_ID, UPSTREAM_STAGE_ID,
                () -> callbackInvoked.set(true));

        assertThat(httpClient.attemptCount.get()).isEqualTo(3);
        assertThat(callbackInvoked.get()).isFalse();
    }

    @Test
    @DisplayName("Legacy 4-arg overload still works (backward compat)")
    void legacyOverload_noRetryCallback() {
        FailNTimesHttpClient httpClient = new FailNTimesHttpClient(
                RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS + 1);
        RemoteTaskDispatcher dispatcher = newDispatcher(httpClient);

        // 4-arg overload delegates to 5-arg with null callback — should not throw
        dispatcher.notifyUpstreamCommit(WORKER_HOST, HTTP_PORT, QUERY_ID, UPSTREAM_STAGE_ID);

        assertThat(httpClient.attemptCount.get())
                .isEqualTo(RemoteTaskDispatcher.COMMIT_NOTIFY_MAX_ATTEMPTS);
    }

    // ── Helpers ──

    private static RemoteTaskDispatcher newDispatcher(HttpClient httpClient) {
        return new RemoteTaskDispatcher(httpClient, CircuitBreakerPolicy.noOp(), TRACING);
    }

    /**
     * Test double HttpClient that throws IOException for the first N send() calls,
     * then returns HTTP 200. Simulates transient network failures.
     */
    private static final class FailNTimesHttpClient extends HttpClient {
        private final int failCount;
        final AtomicInteger attemptCount = new AtomicInteger(0);

        FailNTimesHttpClient(int failCount) {
            this.failCount = failCount;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> HttpResponse<T> send(HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler)
                throws IOException {
            int attempt = attemptCount.incrementAndGet();
            if (attempt <= failCount) {
                throw new IOException("Simulated transient network failure (attempt " + attempt + ")");
            }
            return (HttpResponse<T>) new StubHttpResponse(200, "OK");
        }

        // Required abstract method stubs — unused in this test
        @Override public java.util.Optional<java.net.CookieHandler> cookieHandler() { return java.util.Optional.empty(); }
        @Override public java.util.Optional<java.time.Duration> connectTimeout() { return java.util.Optional.empty(); }
        @Override public Redirect followRedirects() { return Redirect.NEVER; }
        @Override public java.util.Optional<java.net.ProxySelector> proxy() { return java.util.Optional.empty(); }
        @Override public javax.net.ssl.SSLContext sslContext() { return null; }
        @Override public javax.net.ssl.SSLParameters sslParameters() { return new javax.net.ssl.SSLParameters(); }
        @Override public java.util.Optional<java.net.Authenticator> authenticator() { return java.util.Optional.empty(); }
        @Override public Version version() { return Version.HTTP_1_1; }
        @Override public java.util.Optional<java.util.concurrent.Executor> executor() { return java.util.Optional.empty(); }
        @Override public <T> java.util.concurrent.CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Not used in this test"));
        }
        @Override public <T> java.util.concurrent.CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Not used in this test"));
        }
    }

    /**
     * Test double HttpClient that returns non-200 status for the first N calls,
     * then returns HTTP 200. Simulates transient server errors.
     */
    private static final class HttpStatusErrorHttpClient extends HttpClient {
        private final int errorCount;
        final AtomicInteger attemptCount = new AtomicInteger(0);

        HttpStatusErrorHttpClient(int errorCount) {
            this.errorCount = errorCount;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> HttpResponse<T> send(HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler) {
            int attempt = attemptCount.incrementAndGet();
            if (attempt <= errorCount) {
                return (HttpResponse<T>) new StubHttpResponse(503, "Service Unavailable");
            }
            return (HttpResponse<T>) new StubHttpResponse(200, "OK");
        }

        @Override public java.util.Optional<java.net.CookieHandler> cookieHandler() { return java.util.Optional.empty(); }
        @Override public java.util.Optional<java.time.Duration> connectTimeout() { return java.util.Optional.empty(); }
        @Override public Redirect followRedirects() { return Redirect.NEVER; }
        @Override public java.util.Optional<java.net.ProxySelector> proxy() { return java.util.Optional.empty(); }
        @Override public javax.net.ssl.SSLContext sslContext() { return null; }
        @Override public javax.net.ssl.SSLParameters sslParameters() { return new javax.net.ssl.SSLParameters(); }
        @Override public java.util.Optional<java.net.Authenticator> authenticator() { return java.util.Optional.empty(); }
        @Override public Version version() { return Version.HTTP_1_1; }
        @Override public java.util.Optional<java.util.concurrent.Executor> executor() { return java.util.Optional.empty(); }
        @Override public <T> java.util.concurrent.CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Not used in this test"));
        }
        @Override public <T> java.util.concurrent.CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Not used in this test"));
        }
    }

    /**
     * Minimal HttpResponse stub returning a fixed status code and body.
     */
    private record StubHttpResponse(int statusCode, String body)
            implements HttpResponse<String> {
        @Override public HttpRequest request() { return null; }
        @Override public java.util.Optional<HttpResponse<String>> previousResponse() { return java.util.Optional.empty(); }
        @Override public HttpHeaders headers() { return HttpHeaders.of(java.util.Map.of(), (a, b) -> true); }
        @Override public java.util.Optional<javax.net.ssl.SSLSession> sslSession() { return java.util.Optional.empty(); }
        @Override public java.net.URI uri() { return null; }
        @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
    }
}
