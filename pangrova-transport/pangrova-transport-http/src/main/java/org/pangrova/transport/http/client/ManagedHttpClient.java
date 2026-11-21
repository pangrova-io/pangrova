package org.pangrova.transport.http.client;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * AutoCloseable wrapper for HttpClient + its backing ExecutorService.
 * Ensures the thread pool is shut down when the client is no longer needed.
 *
 * @param httpClient managed HTTP client
 * @param executor backing executor owned by this client
 */
public record ManagedHttpClient(HttpClient httpClient, ExecutorService executor) implements AutoCloseable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    /**
     * Stops admission and waits for the owned executor to actually terminate.
     * A failed close retains this owner so its caller can retry after draining work.
     *
     * @throws IllegalStateException if work has not exited or the join is interrupted
     */
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Managed HTTP executor has not exited");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Managed HTTP executor drain interrupted", e);
        }
    }
}
