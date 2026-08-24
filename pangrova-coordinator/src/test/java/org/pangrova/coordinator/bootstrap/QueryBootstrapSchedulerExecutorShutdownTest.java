package org.pangrova.coordinator.bootstrap;

import org.pangrova.observability.noop.NoOpTracingProvider;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link QueryBootstrap#shutdown()} shuts down the schedulerExecutor
 * as a fallback to prevent thread leaks when HA bootstrap fails (R7.2).
 */
class QueryBootstrapSchedulerExecutorShutdownTest {

    @Test
    void shutdownTerminatesSchedulerExecutor() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("exec-shutdown-1", 0);
        QueryBootstrap bootstrap = new QueryBootstrap(
                config, new NoOpTracingProvider(), null, null, null,
                new ModuleIntegrationBootstrap(), null);

        bootstrap.start();

        // Force lazy creation of schedulerExecutor
        ScheduledExecutorService executor = bootstrap.schedulerExecutor();
        assertThat(executor.isShutdown()).isFalse();

        bootstrap.shutdown();

        assertThat(executor.isShutdown())
                .as("schedulerExecutor should be shut down after QueryBootstrap.shutdown()")
                .isTrue();
    }

    @Test
    void shutdownIsIdempotentWhenSchedulerExecutorIsNull() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("exec-shutdown-2", 0);
        QueryBootstrap bootstrap = new QueryBootstrap(
                config, new NoOpTracingProvider(), null, null, null,
                new ModuleIntegrationBootstrap(), null);

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
