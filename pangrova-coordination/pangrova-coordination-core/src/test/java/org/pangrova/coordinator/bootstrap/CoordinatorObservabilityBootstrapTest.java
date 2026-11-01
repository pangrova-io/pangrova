package org.pangrova.coordinator.bootstrap;

import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.observability.sdk.tracing.otel.OtelTracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.pangrova.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Integration tests for observability bootstrap in Coordinator.
 * Validates NoOp fallback and real OTel initialization paths.
 */
class CoordinatorObservabilityBootstrapTest {

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null) {
            bootstrap.shutdown();
        }
    }

    @Test
    void disabledObservabilityUsesNoOp() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-obs-1", 0);
        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getTracingProvider()).isInstanceOf(NoOpTracingProvider.class);
    }

    @Test
    void enabledWithoutOtelConfigFallsBackToNoOp() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-obs-2", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        true, false, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);
        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getTracingProvider()).isInstanceOf(NoOpTracingProvider.class);
    }

    @Test
    void enabledWithOtelConfigCreatesRealProviders() {
        var obsCfg = new CoordinatorBootstrapConfig.ObservabilityBootstrapConfig(
                "pangrova-coordinator", "http://localhost:4317");
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-obs-3", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        true, false, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, obsCfg);
        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getTracingProvider()).isInstanceOf(OtelTracingProvider.class);
    }

    @Test
    void otelShutdownCompletesCleanly() {
        var obsCfg = new CoordinatorBootstrapConfig.ObservabilityBootstrapConfig(
                "pangrova-coordinator", "http://localhost:4317");
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-obs-4", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        true, false, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, obsCfg);
        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThatCode(() -> bootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }
}
