package org.pangrova.worker.ai.context;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/** Joins actual client ownership; Future cancellation alone is never an exit proof. */
final class AiHttpExchange implements AiProviderCallContext.LocalExchange {
    private final Thread sender = Thread.currentThread();
    private final AiBoundedBodySubscriber body;
    private final CountDownLatch terminated = new CountDownLatch(1);
    private CompletableFuture<HttpResponse<String>> response;
    private boolean cancelled;

    AiHttpExchange(AiProviderCallContext.PhysicalAttempt attempt) {
        body = new AiBoundedBodySubscriber(attempt);
    }

    synchronized CompletableFuture<HttpResponse<String>> start(HttpClient client, HttpRequest request)
            throws IOException {
        if (cancelled) {
            throw new IOException("AI client exchange was cancelled before transport submission");
        }
        response = client.sendAsync(request, info -> body);
        return response;
    }

    @Override
    public synchronized void cancel() {
        cancelled = true;
        // The subscriber monitor joins any onNext currently retaining/charging a buffer.
        body.abort(new IOException("AI client exchange was cancelled"));
        if (response != null) {
            response.cancel(true);
        }
    }

    void finish() {
        terminated.countDown();
    }

    @Override
    public void awaitTermination() {
        if ((Thread.currentThread() == sender || body.deliveringOnCurrentThread())
                && terminated.getCount() != 0) {
            throw new IllegalStateException("An AI sending thread or body callback cannot join its own exchange");
        }
        boolean interrupted = false;
        while (true) {
            try {
                terminated.await();
                break;
            } catch (InterruptedException failure) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
