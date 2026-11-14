package org.pangrova.coordinator.resilience.degradation;

import java.util.List;

import org.pangrova.coordinator.resilience.degradation.GracefulDegradationController.AdmissionDecision;
import org.pangrova.coordinator.resilience.degradation.GracefulDegradationController.AdmissionRequest;
import org.pangrova.coordinator.resilience.degradation.GracefulDegradationController.QueryLoadEntry;
import org.pangrova.coordinator.resilience.degradation.GracefulDegradationController.RejectionReason;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GracefulDegradationControllerTest {

    private final GracefulDegradationController controller =
            new GracefulDegradationController(0.90, 0.85, 100);

    @Test
    void admitsQueryWhenBelowAllThresholds() {
        AdmissionRequest request = new AdmissionRequest("tenant-1", 5, 0.50, 0.40, 10);
        AdmissionDecision decision = controller.evaluateAdmission(request);

        assertThat(decision.admitted()).isTrue();
        assertThat(decision.reason()).isNull();
    }

    @Test
    void rejectsQueryWhenMemoryPressureExceeded() {
        AdmissionRequest request = new AdmissionRequest("tenant-1", 5, 0.95, 0.40, 10);
        AdmissionDecision decision = controller.evaluateAdmission(request);

        assertThat(decision.admitted()).isFalse();
        assertThat(decision.reason()).isEqualTo(RejectionReason.MEMORY_PRESSURE);
    }

    @Test
    void rejectsQueryWhenCpuPressureExceeded() {
        AdmissionRequest request = new AdmissionRequest("tenant-1", 5, 0.50, 0.90, 10);
        AdmissionDecision decision = controller.evaluateAdmission(request);

        assertThat(decision.admitted()).isFalse();
        assertThat(decision.reason()).isEqualTo(RejectionReason.CPU_PRESSURE);
    }

    @Test
    void rejectsQueryWhenQueueFull() {
        AdmissionRequest request = new AdmissionRequest("tenant-1", 5, 0.50, 0.40, 100);
        AdmissionDecision decision = controller.evaluateAdmission(request);

        assertThat(decision.admitted()).isFalse();
        assertThat(decision.reason()).isEqualTo(RejectionReason.QUEUE_FULL);
    }

    @Test
    void queueFullTakesPrecedenceOverOtherPressures() {
        AdmissionRequest request = new AdmissionRequest("tenant-1", 5, 0.95, 0.90, 100);
        AdmissionDecision decision = controller.evaluateAdmission(request);

        assertThat(decision.admitted()).isFalse();
        assertThat(decision.reason()).isEqualTo(RejectionReason.QUEUE_FULL);
    }

    @Test
    void selectQueriesToShedOrdersByPriorityAscending() {
        List<QueryLoadEntry> pending = List.of(
                new QueryLoadEntry("q-1", "high-tenant", 10),
                new QueryLoadEntry("q-2", "low-tenant", 1),
                new QueryLoadEntry("q-3", "mid-tenant", 5));

        List<QueryLoadEntry> toShed = controller.selectQueriesToShed(pending);

        assertThat(toShed).extracting(QueryLoadEntry::tenantPriority)
                .containsExactly(1, 5, 10);
    }

    @Test
    void selectQueriesToShedReturnsEmptyForNullInput() {
        assertThat(controller.selectQueriesToShed(null)).isEmpty();
    }

    @Test
    void selectQueriesToShedReturnsEmptyForEmptyInput() {
        assertThat(controller.selectQueriesToShed(List.of())).isEmpty();
    }

    @Test
    void invalidMemoryThresholdThrowsException() {
        assertThatThrownBy(() -> new GracefulDegradationController(0.0, 0.85, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GracefulDegradationController(1.5, 0.85, 100))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidCpuThresholdThrowsException() {
        assertThatThrownBy(() -> new GracefulDegradationController(0.90, 0.0, 100))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidMaxQueueDepthThrowsException() {
        assertThatThrownBy(() -> new GracefulDegradationController(0.90, 0.85, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
