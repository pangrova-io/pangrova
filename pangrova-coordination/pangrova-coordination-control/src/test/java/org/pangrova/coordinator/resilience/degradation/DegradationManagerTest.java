package org.pangrova.coordinator.resilience.degradation;

import org.pangrova.coordinator.enums.DegradationLevel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests DegradationManager health evaluation and query admission logic.
 */
class DegradationManagerTest {

    private final DegradationManager manager = new DegradationManager();

    @Test
    void healthySystem_returnsNone() {
        DegradationLevel level = manager.evaluateDegradation(1.0, 0.3);
        assertThat(level).isEqualTo(DegradationLevel.NONE);
    }

    @Test
    void lowWorkerRatio_returnsCritical() {
        DegradationLevel level = manager.evaluateDegradation(0.05, 0.5);
        assertThat(level).isEqualTo(DegradationLevel.CRITICAL);
    }

    @Test
    void highMemoryPressure_returnsCritical() {
        DegradationLevel level = manager.evaluateDegradation(0.8, 0.96);
        assertThat(level).isEqualTo(DegradationLevel.CRITICAL);
    }

    @Test
    void moderateWorkerShortage_returnsSevere() {
        DegradationLevel level = manager.evaluateDegradation(0.2, 0.5);
        assertThat(level).isEqualTo(DegradationLevel.SEVERE);
    }

    @Test
    void highMemoryPressureModerate_returnsModerate() {
        DegradationLevel level = manager.evaluateDegradation(0.8, 0.92);
        assertThat(level).isEqualTo(DegradationLevel.MODERATE);
    }

    @Test
    void slightlyReducedWorkers_returnsLight() {
        DegradationLevel level = manager.evaluateDegradation(0.6, 0.5);
        assertThat(level).isEqualTo(DegradationLevel.LIGHT);
    }

    @Test
    void checkQueryAllowed_criticalLevel_rejects() {
        assertThat(manager.checkQueryAllowed(DegradationLevel.CRITICAL)).isFalse();
    }

    @Test
    void checkQueryAllowed_nonCriticalLevels_allows() {
        assertThat(manager.checkQueryAllowed(DegradationLevel.NONE)).isTrue();
        assertThat(manager.checkQueryAllowed(DegradationLevel.LIGHT)).isTrue();
        assertThat(manager.checkQueryAllowed(DegradationLevel.MODERATE)).isTrue();
        assertThat(manager.checkQueryAllowed(DegradationLevel.SEVERE)).isTrue();
    }
}
