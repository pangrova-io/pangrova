package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link QueryBootstrap#shutdown()} shuts down the schedulerExecutor
 * as a fallback to prevent thread leaks when HA bootstrap fails (R7.2).
 */
class QueryBootstrapSchedulerExecutorShutdownTest {

    @Test
    void shutdownTerminatesSchedulerExecutor() throws Exception {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("exec-shutdown-1", 0);
        QueryBootstrap bootstrap = new QueryBootstrap(
                config, new NoOpTracingProvider(), null, null, null,
                new ModuleIntegrationBootstrap(), null, ModelAuthorityFixtures.models());

        bootstrap.start();

        // Force lazy creation of schedulerExecutor
        ScheduledExecutorService executor = bootstrap.schedulerExecutor();
        assertThat(executor.isShutdown()).isFalse();
        var actualCallback = new CountDownLatch(1);
        executor.execute(actualCallback::countDown);
        assertThat(actualCallback.await(3, TimeUnit.SECONDS)).isTrue();

        bootstrap.shutdown();

        assertThat(executor.isShutdown())
                .as("schedulerExecutor should be shut down after QueryBootstrap.shutdown()")
                .isTrue();
        assertThat(executor.isTerminated())
                .as("shutdown returns only after the actual owned executor has exited")
                .isTrue();
        assertThatThrownBy(() -> executor.execute(() -> {
            throw new AssertionError("retired scheduler accepted new work");
        })).isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void shutdownIsIdempotentWhenSchedulerExecutorIsNull() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("exec-shutdown-2", 0);
        QueryBootstrap bootstrap = new QueryBootstrap(
                config, new NoOpTracingProvider(), null, null, null,
                new ModuleIntegrationBootstrap(), null, ModelAuthorityFixtures.models());

        // Do NOT call start() — schedulerExecutor remains null
        // shutdown() should not throw
        bootstrap.shutdown();
    }

    @Test
    void shutdownHelperHandlesNullExecutorGracefully() {
        // Direct test of the helper method with null
        CoordinatorShutdownSequence.shutdownSchedulerExecutor(null);
        // No exception = pass
    }
}
