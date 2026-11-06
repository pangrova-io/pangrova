package org.pangrova.transport.client.http;

import org.junit.jupiter.api.Test;
import org.pangrova.client.session.ConnectionPoolConfig;
import org.pangrova.client.session.RetryPolicy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual configured executor ownership; no business HTTP or query behavior is fabricated. */
class RestHttpClientOwnerTest {
    @Test void explicitThreadsAreConsumedWithoutFailoverEndpointMultiplication() {
        var config = new PangrovaRestClientConfig("localhost", 8080, java.time.Duration.ofSeconds(2),
                java.time.Duration.ofSeconds(2), false, null, null, null, RetryPolicy.DEFAULT,
                ConnectionPoolConfig.withFailover(java.util.List.of("localhost:8080", "backup:8081")), 2, null);
        var owner = RestHttpClientOwner.create(config, null);
        var executor = (java.util.concurrent.ThreadPoolExecutor) owner.client().executor().orElseThrow();
        try {
            assertThat(executor.getCorePoolSize()).isEqualTo(2);
            assertThat(executor.getMaximumPoolSize()).isEqualTo(2);
            assertThat(config.connectionPoolConfig().maxConnectionsPerEndpoint()).isEqualTo(10);
        } finally { owner.close(); }
        assertThat(executor.isTerminated()).isTrue();
    }

    @Test void borrowedClientCannotSilentlyIgnoreAnExplicitExecutorSize() {
        assertThatThrownBy(() -> PangrovaRestClient.builder().httpClient(java.net.http.HttpClient.newHttpClient())
                .httpExecutorThreads(2).build()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("configured HTTP executor size");
    }

    @Test void actualOwnedExecutorIsTerminatedBeforeSuccessfulCloseReturns() {
        var owner = RestHttpClientOwner.create(config(), null);
        var executor = (ExecutorService) owner.client().executor().orElseThrow();
        owner.close();
        assertThat(executor.isTerminated()).isTrue();
        assertThatThrownBy(owner::enter).isInstanceOf(IllegalStateException.class);
    }

    @Test void interruptedExecutorCloseRetainsOwnerAndRestoresInterruptForRetry() throws Exception {
        var owner = RestHttpClientOwner.create(config(), null);
        var executor = (ExecutorService) owner.client().executor().orElseThrow();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        executor.execute(() -> {
            entered.countDown();
            try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("Actual executor remains held"); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
        });
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("ownership is retained");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(executor.isTerminated()).isFalse();
        } finally {
            Thread.interrupted();
            release.countDown();
            owner.close();
        }
        assertThat(executor.isTerminated()).isTrue();
    }

    private static PangrovaRestClientConfig config() {
        return new PangrovaRestClientConfig("localhost", 8080, java.time.Duration.ofSeconds(2), java.time.Duration.ofSeconds(2),
                false, null, null, null, RetryPolicy.DEFAULT, ConnectionPoolConfig.singleEndpoint("localhost", 8080), null, null);
    }
}
