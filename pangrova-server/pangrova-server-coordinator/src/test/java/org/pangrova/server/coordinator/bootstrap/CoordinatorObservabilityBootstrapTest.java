package org.pangrova.server.coordinator.bootstrap;

import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;

import org.pangrova.observability.sdk.config.ObservabilityConfig;

import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.sdk.tracing.otel.OtelTracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

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
                        "coord-obs-2", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole.COMBINED, "0.0.0.0"),
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
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.port", "0",
                "observability.service-name", "pangrova-coordinator",
                "observability.otlp-endpoint", "http://localhost:4317",
                "observability.sampling.probability", "0.25")));
        assertThat(config.observabilityBootstrapConfig().getSamplingProbability()).isEqualTo(0.25);
        var phase = new ObservabilityBootstrap(config);
        try {
            phase.start();
            assertThat(phase.tracingProvider()).isInstanceOf(OtelTracingProvider.class);
        } finally {
            phase.shutdown();
        }
    }

    @Test
    void otelShutdownCompletesCleanly() {
        var obsCfg = new ObservabilityConfig(PangrovaConfiguration.fromYaml(Map.of(
                "observability.service-name", "pangrova-coordinator", "observability.otlp-endpoint", "http://localhost:4317")));
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "coord-obs-4", 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole.COMBINED, "0.0.0.0"),
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
