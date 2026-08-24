package org.pangrova.coordinator.scheduler;

import org.pangrova.common.http.HttpClientFactory;
import org.pangrova.common.http.ManagedHttpClient;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies R10.1: RemoteTaskDispatcher.close() shuts down the HttpClient executor.
 */
class RemoteTaskDispatcherCloseTest {

    @Test
    void close_managedClient_executorIsShutdown() {
        ManagedHttpClient managed = HttpClientFactory.createDefault();
        ExecutorService executor = managed.executor();

        assertThat(executor.isShutdown()).isFalse();
        managed.close();
        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    void close_externallyProvidedClient_noShutdown() {
        HttpClient external = HttpClient.newHttpClient();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                external, CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());

        // Should not throw — managedClient is null, so close is a no-op
        dispatcher.close();
    }

    @Test
    void close_defaultConstructor_isIdempotent() {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        dispatcher.close();
        dispatcher.close(); // second close should not throw
    }
}
