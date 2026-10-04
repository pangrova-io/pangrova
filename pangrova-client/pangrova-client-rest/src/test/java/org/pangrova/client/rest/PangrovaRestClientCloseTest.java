package org.pangrova.client.rest;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies R10.5 and R10.6:
 * - R10.5: Shared HttpClient is NOT closed by PangrovaRestClient.close()
 * - R10.6: Owned HttpClient executor IS shut down by PangrovaRestClient.close()
 */
class PangrovaRestClientCloseTest {

    @Test
    void close_ownedClient_shutsDownExecutor() {
        // When no shared HttpClient is provided, PangrovaRestClient creates its own.
        // close() should shut down the internal executor.
        PangrovaRestClient client = PangrovaRestClient.builder()
                .coordinatorHost("localhost")
                .coordinatorPort(8080)
                .connectionPoolConfig(ConnectionPoolConfig.singleEndpoint("localhost", 8080))
                .build();

        // close should not throw
        client.close();
        assertThatThrownBy(() -> client.ping(1)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("admission is closed");
        // Calling close again should be idempotent
        client.close();
    }

    @Test
    void close_sharedClient_doesNotShutDownExecutor() {
        AtomicInteger counter = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "test-shared-http-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
        HttpClient sharedHttpClient = HttpClient.newBuilder()
                .executor(executor)
                .build();

        PangrovaRestClient client = PangrovaRestClient.builder()
                .coordinatorHost("localhost")
                .coordinatorPort(8080)
                .httpClient(sharedHttpClient)
                .build();

        client.close();

        // Shared executor should NOT be shut down by PangrovaRestClient.close()
        assertThat(executor.isShutdown()).isFalse();

        // Clean up
        executor.shutdown();
    }

    @Test
    void close_isIdempotent() {
        PangrovaRestClient client = PangrovaRestClient.builder()
                .coordinatorHost("localhost")
                .coordinatorPort(9999)
                .build();

        client.close();
        client.close();
        client.close();
        assertThatThrownBy(() -> client.ping(1)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("admission is closed");
    }
}
