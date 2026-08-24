package org.pangrova.common.http;

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

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
