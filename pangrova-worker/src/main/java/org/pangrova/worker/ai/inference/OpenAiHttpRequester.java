package org.pangrova.worker.ai.inference;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.worker.ai.context.AiProviderCallContext;
import org.pangrova.worker.ai.context.AiHttpTransport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Governed HTTP transport shared by the exact OpenAI wire dialects. */
final class OpenAiHttpRequester implements AutoCloseable {

    private static final int MAX_ATTEMPTS = 4;
    private static final long INITIAL_BACKOFF_MILLIS = 500;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final int HTTP_OK = 200;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_REQUEST_TIMEOUT = 408;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_UNPROCESSABLE_CONTENT = 422;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_INTERNAL_ERROR = 500;
    private static final int HTTP_GATEWAY_TIMEOUT = 504;
    private static final int HTTP_SERVER_ERROR_MAX = 599;

    private final URI endpoint;
    private final String apiKey;
    private final Duration timeout;
    private final HttpClient client;
    private final Semaphore rateLimiter;
    private final int ratePermits;
    private final ScheduledExecutorService replenisher;
    private final ExecutorService httpExecutor;

    OpenAiHttpRequester(OpenAiInferenceConfig config, String threadPrefix) {
        this.endpoint = URI.create(config.endpoint());
        this.apiKey = config.apiKey();
        this.timeout = Duration.ofSeconds(config.timeoutSeconds());
        this.ratePermits = Math.max(1, (int) Math.min(
                Integer.MAX_VALUE, Math.ceil(config.maxRequestsPerSecond())));
        this.rateLimiter = new Semaphore(ratePermits, true);
        this.httpExecutor = ManagedExecutorFactory.newFixedThreadPool(
                threadPrefix + "-http", config.maxConcurrency());
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(httpExecutor)
                .build();
        this.replenisher = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                threadPrefix + "-rate-replenish");
        long intervalNanos = Math.max(
                1L, Math.round(NANOS_PER_SECOND / config.maxRequestsPerSecond()));
        replenisher.scheduleAtFixedRate(
                this::replenish, intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
    }

    String postJson(String body) {
        return post("application/json", body.getBytes(StandardCharsets.UTF_8));
    }

    String post(String contentType, byte[] body) {
        long backoffMillis = INITIAL_BACKOFF_MILLIS;
        AiInvocationException lastFailure = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (attempt > 1) {
                waitBeforeRetry(backoffMillis);
                backoffMillis = Math.multiplyExact(backoffMillis, 2L);
            }
            acquirePermit();
            try {
                return send(contentType, body);
            } catch (AiInvocationException failure) {
                if (!failure.retryable() || attempt == MAX_ATTEMPTS) {
                    throw failure;
                }
                lastFailure = failure;
            }
        }
        throw lastFailure;
    }

    private String send(String contentType, byte[] body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(AiProviderCallContext.remainingTimeout(timeout))
                .header("Accept", "application/json")
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (!apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        AiProviderCallContext.currentIdempotencyKey().ifPresent(
                value -> builder.header("Idempotency-Key", value));
        try {
            HttpResponse<String> response = AiHttpTransport.send(client, builder.build());
            if (response.statusCode() == HTTP_OK) { return response.body(); }
            throw statusFailure(response.statusCode());
        } catch (HttpTimeoutException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    true,
                    "AI provider HTTP request timed out",
                    failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    false,
                    "AI provider HTTP request was interrupted",
                    failure);
        } catch (IOException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_UNAVAILABLE,
                    true,
                    "AI provider HTTP request failed",
                    failure);
        }
    }

    private void acquirePermit() {
        try {
            long waitMillis = AiProviderCallContext.remainingTimeout(timeout).toMillis();
            if (!rateLimiter.tryAcquire(Math.max(1L, waitMillis), TimeUnit.MILLISECONDS)) {
                throw new AiInvocationException(
                        AiInvocationException.Category.RATE_LIMIT,
                        true,
                        "Timed out waiting for AI provider admission");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    false,
                    "AI provider admission was interrupted",
                    failure);
        }
    }

    private static AiInvocationException statusFailure(int statusCode) {
        AiInvocationException.Category category = switch (statusCode) {
            case HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_CONFLICT,
                    HTTP_UNPROCESSABLE_CONTENT -> AiInvocationException.Category.INVALID_REQUEST;
            case HTTP_UNAUTHORIZED -> AiInvocationException.Category.AUTHENTICATION;
            case HTTP_FORBIDDEN -> AiInvocationException.Category.AUTHORIZATION;
            case HTTP_REQUEST_TIMEOUT, HTTP_GATEWAY_TIMEOUT ->
                    AiInvocationException.Category.TIMEOUT;
            case HTTP_TOO_MANY_REQUESTS -> AiInvocationException.Category.RATE_LIMIT;
            default -> AiInvocationException.Category.PROVIDER_UNAVAILABLE;
        };
        boolean retryable = statusCode == HTTP_REQUEST_TIMEOUT
                || statusCode == HTTP_GATEWAY_TIMEOUT
                || statusCode == HTTP_TOO_MANY_REQUESTS
                || statusCode >= HTTP_INTERNAL_ERROR
                && statusCode <= HTTP_SERVER_ERROR_MAX;
        return new AiInvocationException(
                category, retryable, "AI provider returned HTTP " + statusCode);
    }

    private static void waitBeforeRetry(long backoffMillis) {
        long jitterBound = Math.max(1L, backoffMillis / 2L);
        long waitMillis = Math.addExact(
                backoffMillis, ThreadLocalRandom.current().nextLong(jitterBound));
        ThreadUtils.sleepQuietly(AiProviderCallContext.remainingWaitMillis(waitMillis));
        if (Thread.currentThread().isInterrupted()) {
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    false,
                    "AI provider retry was interrupted");
        }
    }

    private void replenish() {
        if (rateLimiter.availablePermits() < ratePermits) {
            rateLimiter.release();
        }
    }

    @Override
    public void close() {
        replenisher.shutdown();
        httpExecutor.shutdown();
    }
}
