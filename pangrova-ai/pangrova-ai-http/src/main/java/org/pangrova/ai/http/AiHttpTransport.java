package org.pangrova.ai.http;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiProviderCallContext;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Governed HTTP entity transport; bytes exclude headers, TLS and TCP retransmissions. */
public final class AiHttpTransport {
    private AiHttpTransport() {}

    /**
     * One admitted application send, retaining all concurrency permits until the body completes.
     */
    public static HttpResponse<String> send(HttpClient client, HttpRequest request)
            throws IOException, InterruptedException {
        HttpRequest.BodyPublisher publisher =
                request.bodyPublisher()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "AI HTTP request requires an encoded entity body"));
        try (var attempt = AiProviderCallContext.openAttempt(publisher.contentLength())) {
            var exchange = new AiHttpExchange(attempt);
            try (var registration = attempt.registerExchange(exchange)) {
                try {
                    return sendAdmitted(client, request, publisher, exchange);
                } finally {
                    exchange.cancel();
                    attempt.close();
                    exchange.finish();
                }
            }
        }
    }

    private static HttpResponse<String> sendAdmitted(
            HttpClient client, HttpRequest request, HttpRequest.BodyPublisher publisher,
            AiHttpExchange exchange)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(request, (name, value) -> true)
                        .method(request.method(), new AiSingleSendBodyPublisher(publisher));
        Duration timeout = AiProviderCallContext.remainingTimeout(request.timeout().orElseThrow(
                () -> new IllegalArgumentException("AI HTTP request requires a bounded timeout")));
        builder.timeout(timeout);
        try {
            return sendThroughBody(client, builder.build(), exchange, timeout);
        } catch (IOException failure) {
            rethrowResourceFailure(failure);
            throw new AiInvocationException(
                    AiFailureCategory.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "AI provider POST outcome is unknown after transport failure",
                    failure);
        }
    }

    private static HttpResponse<String> sendThroughBody(
            HttpClient client, HttpRequest request, AiHttpExchange exchange, Duration timeout)
            throws IOException, InterruptedException {
        long timeoutNanos = timeout.toNanos();
        long started = System.nanoTime();
        var response = exchange.start(client, request);
        try {
            // JDK 17's request timeout ends at response headers, not at the end of the body.
            long remaining = timeoutNanos - (System.nanoTime() - started);
            return response.get(Math.max(0L, remaining), TimeUnit.NANOSECONDS);
        } catch (TimeoutException expired) {
            var failure = new HttpTimeoutException("AI provider response body exceeded its deadline");
            exchange.cancel();
            throw failure;
        } catch (InterruptedException interrupted) {
            exchange.cancel();
            throw interrupted;
        } catch (CancellationException cancelled) {
            throw new IOException("AI provider POST was cancelled with an unknown remote outcome", cancelled);
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof CancellationException) {
                throw new IOException("AI provider POST was cancelled with an unknown remote outcome", cause);
            }
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof AiInvocationException typed
                    && typed.category() == AiFailureCategory.AUTHORIZATION) {
                throw new IOException("AI task authority was revoked after provider submission", typed);
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("AI provider transport failed", cause);
        }
    }

    private static void rethrowResourceFailure(IOException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof AiInvocationException typed
                    && typed.category() != AiFailureCategory.AUTHORIZATION) {
                throw typed;
            }
        }
    }
}
