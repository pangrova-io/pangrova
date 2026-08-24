package org.pangrova.common.http;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for creating managed HTTP client instances with connection pooling.
 * Returns ManagedHttpClient so callers can shut down the backing thread pool.
 */
public final class HttpClientFactory {

    private HttpClientFactory() {}

    /** Creates a ManagedHttpClient with the given configuration. */
    public static ManagedHttpClient create(HttpClientConfig config) {
        int poolSize = Math.min(config.maxConnections(), Runtime.getRuntime().availableProcessors());
        String threadPrefix = config.name() + "-http-client";
        AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, threadPrefix + "-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(config.connectTimeout())
                .executor(executor)
                .build();
        return new ManagedHttpClient(client, executor);
    }

    /** Creates a ManagedHttpClient with default configuration (64 connections, 5s connect, 30s request). */
    public static ManagedHttpClient createDefault() {
        return create(HttpClientConfig.DEFAULT);
    }
}
