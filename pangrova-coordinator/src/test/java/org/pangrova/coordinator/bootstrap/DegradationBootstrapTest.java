package org.pangrova.coordinator.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.resilience.degradation.DegradationManager;
import org.pangrova.resilience.degradation.DegradationManager.DegradationLevel;
import org.pangrova.resilience.degradation.DegradationThresholds;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/**
 * Verifies DegradationManager evaluation with simulated health metrics
 * and query admission gating in bootstrap context.
 */
class DegradationBootstrapTest {

    @Test
    void healthyClusterAllowsQueries() {
        DegradationManager manager = new DegradationManager();

        DegradationLevel level = manager.evaluateDegradation(1.0, 0.0);

        assertThat(level).isEqualTo(DegradationLevel.NONE);
        assertThat(manager.checkQueryAllowed(level)).isTrue();
    }

    @Test
    void criticalDegradationRejectsQueries() {
        DegradationManager manager = new DegradationManager();

        DegradationLevel level = manager.evaluateDegradation(0.05, 0.99);

        assertThat(level).isEqualTo(DegradationLevel.CRITICAL);
        assertThat(manager.checkQueryAllowed(level)).isFalse();
    }

    @Test
    void moderateDegradationStillAllowsQueries() {
        DegradationManager manager = new DegradationManager();

        DegradationLevel level = manager.evaluateDegradation(0.8, 0.92);

        assertThat(level).isEqualTo(DegradationLevel.MODERATE);
        assertThat(manager.checkQueryAllowed(level)).isTrue();
    }

    @Test
    void lowWorkerRatioEscalatesToSevere() {
        DegradationManager manager = new DegradationManager();

        DegradationLevel level = manager.evaluateDegradation(0.2, 0.5);

        assertThat(level).isEqualTo(DegradationLevel.SEVERE);
        assertThat(manager.checkQueryAllowed(level)).isTrue();
    }

    // -- R2.3: JVM memory pressure supplier tests --

    @Test
    void jvmMemoryPressureSupplierReturnsBoundedRatio() {
        DoubleSupplier jvmMemoryPressure = () -> {
            Runtime rt = Runtime.getRuntime();
            long used = rt.totalMemory() - rt.freeMemory();
            return (double) used / rt.maxMemory();
        };

        double pressure = jvmMemoryPressure.getAsDouble();

        assertThat(pressure).isBetween(0.0, 1.0);
    }

    @Test
    void evaluateCurrentReturnsDegradedWhenMemoryPressureHigh() {
        // Simulate >80% memory pressure via supplier — thresholds: light=0.7, moderate=0.9
        DegradationManager manager = new DegradationManager(
                DegradationThresholds.defaults(),
                () -> 1.0,   // healthy worker ratio
                () -> 0.85   // 85% memory pressure — above light threshold (0.7)
        );

        DegradationLevel level = manager.evaluateCurrent();

        assertThat(level).isNotEqualTo(DegradationLevel.NONE);
        assertThat(level).isEqualTo(DegradationLevel.LIGHT);
    }

    @Test
    void evaluateCurrentReturnsModerateWhenMemoryPressureAbove90Percent() {
        DegradationManager manager = new DegradationManager(
                DegradationThresholds.defaults(),
                () -> 1.0,
                () -> 0.92   // above moderate threshold (0.9)
        );

        DegradationLevel level = manager.evaluateCurrent();

        assertThat(level).isEqualTo(DegradationLevel.MODERATE);
    }

    @Test
    void evaluateCurrentReturnsCriticalWhenMemoryPressureAbove95Percent() {
        DegradationManager manager = new DegradationManager(
                DegradationThresholds.defaults(),
                () -> 1.0,
                () -> 0.96   // above critical threshold (0.95)
        );

        DegradationLevel level = manager.evaluateCurrent();

        assertThat(level).isEqualTo(DegradationLevel.CRITICAL);
        assertThat(manager.checkQueryAllowed(level)).isFalse();
    }

    @Test
    void evaluateCurrentReturnsNoneWhenMemoryPressureLow() {
        DegradationManager manager = new DegradationManager(
                DegradationThresholds.defaults(),
                () -> 1.0,
                () -> 0.3    // well below any threshold
        );

        DegradationLevel level = manager.evaluateCurrent();

        assertThat(level).isEqualTo(DegradationLevel.NONE);
    }

    @Test
    void bindDegradationMetricsLateBindsMemoryPressureSupplier() {
        // Verify that ModuleIntegrationWiring.bindDegradationMetrics() replaces
        // the default 0.0 supplier with a live one, and evaluateCurrent() reflects it.
        try (ModuleIntegrationWiring wiring = new ModuleIntegrationWiring()) {
            CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                    new CoordinatorBootstrapConfig.CoreConfig(
                            "degrade-test", 0,
                            CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS),
                    new CoordinatorBootstrapConfig.FeatureFlags(
                            false, false, false, false, false, true, false, false),
                    CoordinatorBootstrapConfig.QueryConfig.defaults(),
                    CoordinatorBootstrapConfig.TransportConfig.defaults(),
                    CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                    null, null, null);
            wiring.startResilience(config);

            // Before binding: default supplier returns 0.0 → NONE
            DegradationLevel beforeBind = wiring.getDegradationManager().evaluateCurrent();
            assertThat(beforeBind).isEqualTo(DegradationLevel.NONE);

            // Bind a high-pressure supplier
            AtomicReference<Double> pressureRef = new AtomicReference<>(0.92);
            wiring.bindDegradationMetrics(null, pressureRef::get);

            // After binding: supplier returns 0.92 → MODERATE
            DegradationLevel afterBind = wiring.getDegradationManager().evaluateCurrent();
            assertThat(afterBind).isEqualTo(DegradationLevel.MODERATE);
        }
    }
}
