package org.pangrova.spi.resource.scaling;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the auto-scaling SPI contracts (Task 144, G-RES-03).
 */
class AutoScalerSpiTest {

    // --- ClusterMetricsSnapshot ---

    @Test
    void metricsSnapshotStoresFields() {
        var snap = new ClusterMetricsSnapshot(10, 5, 0.75, 0.60, 0.80);
        assertThat(snap.currentWorkers()).isEqualTo(10);
        assertThat(snap.queueDepth()).isEqualTo(5);
        assertThat(snap.avgCpuUtilisation()).isEqualTo(0.75);
        assertThat(snap.avgMemoryUtilisation()).isEqualTo(0.60);
        assertThat(snap.maxTenantPressure()).isEqualTo(0.80);
    }

    @Test
    void metricsSnapshotRejectsNegativeWorkers() {
        assertThatThrownBy(() -> new ClusterMetricsSnapshot(-1, 0, 0.0, 0.0, 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currentWorkers");
    }

    @Test
    void metricsSnapshotRejectsNegativeQueueDepth() {
        assertThatThrownBy(() -> new ClusterMetricsSnapshot(1, -1, 0.0, 0.0, 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queueDepth");
    }

    @Test
    void metricsSnapshotRejectsCpuOutOfRange() {
        assertThatThrownBy(() -> new ClusterMetricsSnapshot(1, 0, 1.5, 0.0, 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("avgCpuUtilisation");
    }

    // --- ScalingDecision ---

    @Test
    void holdDecisionFactory() {
        ScalingDecision d = ScalingDecision.hold(5);
        assertThat(d.direction()).isEqualTo(ScalingDirection.HOLD);
        assertThat(d.targetWorkers()).isEqualTo(5);
        assertThat(d.reason()).isNotBlank();
    }

    @Test
    void scaleOutDecisionFactory() {
        ScalingDecision d = ScalingDecision.scaleOut(10, "high queue depth");
        assertThat(d.direction()).isEqualTo(ScalingDirection.SCALE_OUT);
        assertThat(d.targetWorkers()).isEqualTo(10);
        assertThat(d.reason()).isEqualTo("high queue depth");
    }

    @Test
    void scaleInDecisionFactory() {
        ScalingDecision d = ScalingDecision.scaleIn(3, "low load");
        assertThat(d.direction()).isEqualTo(ScalingDirection.SCALE_IN);
        assertThat(d.targetWorkers()).isEqualTo(3);
    }

    @Test
    void suspendDecisionFactory() {
        ScalingDecision d = ScalingDecision.suspend(1, "idle for 10m");
        assertThat(d.direction()).isEqualTo(ScalingDirection.SUSPEND);
        assertThat(d.targetWorkers()).isEqualTo(1);
    }

    @Test
    void scalingDecisionRejectsNegativeTarget() {
        assertThatThrownBy(() -> new ScalingDecision(ScalingDirection.SCALE_OUT, -1, "bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetWorkers");
    }

    @Test
    void scalingDecisionRejectsNullDirection() {
        assertThatThrownBy(() -> new ScalingDecision(null, 5, "ok"))
                .isInstanceOf(NullPointerException.class);
    }

    // --- AutoScaler interface ---

    @Test
    void autoScalerInterfaceCanBeImplemented() {
        AutoScaler scaler = new AutoScaler() {
            @Override
            public ScalingDecision evaluate(ClusterMetricsSnapshot metrics) {
                if (metrics.queueDepth() > 10) {
                    return ScalingDecision.scaleOut(
                            metrics.currentWorkers() + 2, "queue > 10");
                }
                return ScalingDecision.hold(metrics.currentWorkers());
            }

            @Override
            public String name() {
                return "test-scaler";
            }
        };

        var idle = new ClusterMetricsSnapshot(5, 0, 0.1, 0.1, 0.0);
        assertThat(scaler.evaluate(idle).direction()).isEqualTo(ScalingDirection.HOLD);

        var busy = new ClusterMetricsSnapshot(5, 15, 0.9, 0.8, 0.7);
        ScalingDecision d = scaler.evaluate(busy);
        assertThat(d.direction()).isEqualTo(ScalingDirection.SCALE_OUT);
        assertThat(d.targetWorkers()).isEqualTo(7);

        assertThat(scaler.name()).isEqualTo("test-scaler");
    }

    // --- ScalingExecutor interface ---

    @Test
    void scalingExecutorInterfaceCanBeImplemented() {
        ScalingExecutor executor = new ScalingExecutor() {
            int lastTarget = -1;

            @Override
            public void execute(ScalingDecision decision) {
                lastTarget = decision.targetWorkers();
            }

            @Override
            public String backend() {
                return "test";
            }
        };

        executor.execute(ScalingDecision.scaleOut(10, "test"));
        assertThat(executor.backend()).isEqualTo("test");
    }

    // --- ScalingException ---

    @Test
    void scalingExceptionWithMessage() {
        ScalingException ex = new ScalingException("K8s API error");
        assertThat(ex.getMessage()).isEqualTo("K8s API error");
    }

    @Test
    void scalingExceptionWithCause() {
        var cause = new RuntimeException("timeout");
        ScalingException ex = new ScalingException("K8s API error", cause);
        assertThat(ex.getCause()).isSameAs(cause);
    }
}
