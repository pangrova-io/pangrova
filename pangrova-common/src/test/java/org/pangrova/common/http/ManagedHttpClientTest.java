package org.pangrova.common.http;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies ManagedHttpClient shuts down executor on close (Task 12.3).
 */
class ManagedHttpClientTest {

    @Test
    void closeShutdownsExecutor() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        HttpClient httpClient = HttpClient.newBuilder().executor(executor).build();
        ManagedHttpClient managed = new ManagedHttpClient(httpClient, executor);

        assertFalse(executor.isShutdown());
        managed.close();
        assertTrue(executor.isShutdown());
        assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
    }

    @Test
    void httpClientAccessorReturnsWrappedClient() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HttpClient httpClient = HttpClient.newBuilder().executor(executor).build();
        ManagedHttpClient managed = new ManagedHttpClient(httpClient, executor);

        assertSame(httpClient, managed.httpClient());
        managed.close();
    }
}
