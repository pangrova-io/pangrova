package org.pangrova.worker.ai.context;

import org.pangrova.spi.ai.AiInvocationException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
            HttpRequest.Builder builder =
                    HttpRequest.newBuilder(request, (name, value) -> true)
                            .method(request.method(), new AiSingleSendBodyPublisher(publisher));
            request.timeout()
                    .ifPresent(
                            timeout ->
                                    builder.timeout(
                                            AiProviderCallContext.remainingTimeout(timeout)));
            try {
                return client.send(builder.build(), info -> new AiBoundedBodySubscriber(attempt));
            } catch (IOException failure) {
                rethrowResourceFailure(failure);
                throw new AiInvocationException(
                        AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                        false,
                        "AI provider POST outcome is unknown after transport failure",
                        failure);
            }
        }
    }

    private static void rethrowResourceFailure(IOException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof AiInvocationException typed) {
                throw typed;
            }
        }
    }
}
