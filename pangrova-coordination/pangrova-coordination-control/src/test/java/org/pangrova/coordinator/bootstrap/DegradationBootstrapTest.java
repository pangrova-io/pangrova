package org.pangrova.coordinator.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.coordinator.resilience.degradation.DegradationManager;
import org.pangrova.coordinator.enums.DegradationLevel;
import org.pangrova.coordinator.resilience.degradation.DegradationThresholds;
import org.junit.jupiter.api.Test;

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
}
