package org.pangrova.coordinator.worker;

import org.pangrova.common.value.MemoryPressure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ClusterResourcePressureMonitor}.
 *
 * <p>Validates G-RES-09: cluster-wide resource pressure evaluation feeds
 * into admission control decisions, providing the telemetry-feedback loop
 * for elasticity/admission.</p>
 */
class ClusterResourcePressureMonitorTest {

    private WorkerManager workerManager;

    @BeforeEach
    void setUp() {
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    @Test
    @DisplayName("No active workers -> reject admission")
    void rejectsWhenNoActiveWorkers() {
        var monitor = new ClusterResourcePressureMonitor(workerManager);

        var snapshot = monitor.evaluate();
        assertThat(snapshot.admissionAllowed()).isFalse();
        assertThat(snapshot.rejectionReason()).contains("no active workers");
        assertThat(monitor.shouldAdmit()).isFalse();
    }

    @Test
    @DisplayName("All workers healthy -> admit")
    void admitsWhenAllWorkersHealthy() {
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.LOW));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w3", "host3", 8080, 9090, MemoryPressure.MEDIUM));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        var snapshot = monitor.evaluate();
        assertThat(snapshot.admissionAllowed()).isTrue();
        assertThat(snapshot.rejectionReason()).isNull();
        assertThat(snapshot.totalActiveWorkers()).isEqualTo(3);
        assertThat(snapshot.criticalCount()).isZero();
        assertThat(snapshot.highCount()).isZero();
    }

    @Test
    @DisplayName("All workers CRITICAL -> unconditional reject")
    void rejectsWhenAllWorkersCritical() {
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.CRITICAL));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.CRITICAL));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        var snapshot = monitor.evaluate();
        assertThat(snapshot.admissionAllowed()).isFalse();
        assertThat(snapshot.rejectionReason()).contains("all").contains("CRITICAL");
        assertThat(snapshot.criticalCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Majority workers stressed (>50%) -> reject")
    void rejectsWhenMajorityStressed() {
        // 3 out of 4 workers under HIGH/CRITICAL = 75% > 50% threshold
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.CRITICAL));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.HIGH));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w3", "host3", 8080, 9090, MemoryPressure.HIGH));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w4", "host4", 8080, 9090, MemoryPressure.NONE));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        var snapshot = monitor.evaluate();
        assertThat(snapshot.admissionAllowed()).isFalse();
        assertThat(snapshot.stressedFraction()).isEqualTo(0.75);
        assertThat(snapshot.criticalCount()).isEqualTo(1);
        assertThat(snapshot.highCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Minority workers stressed (<=50%) -> admit")
    void admitsWhenMinorityStressed() {
        // 1 out of 4 workers under HIGH = 25% < 50% threshold
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.HIGH));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w3", "host3", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w4", "host4", 8080, 9090, MemoryPressure.LOW));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        var snapshot = monitor.evaluate();
        assertThat(snapshot.admissionAllowed()).isTrue();
        assertThat(snapshot.stressedFraction()).isEqualTo(0.25);
    }

    @Test
    @DisplayName("Exactly at threshold (50%) -> admit (threshold is exclusive)")
    void admitsAtExactThreshold() {
        // 2 out of 4 workers under HIGH = 50% = threshold (not exceeded)
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.HIGH));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.HIGH));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w3", "host3", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w4", "host4", 8080, 9090, MemoryPressure.NONE));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        var snapshot = monitor.evaluate();
        // 0.5 > 0.5 is false, so admission is allowed at the exact threshold
        assertThat(snapshot.admissionAllowed()).isTrue();
    }

    @Test
    @DisplayName("Custom threshold: stricter rejection")
    void customThresholdStricterRejection() {
        // 1 out of 4 under HIGH = 25%, with threshold 0.2 -> reject
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.HIGH));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w3", "host3", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w4", "host4", 8080, 9090, MemoryPressure.NONE));

        var monitor = new ClusterResourcePressureMonitor(workerManager, 0.2);

        var snapshot = monitor.evaluate();
        assertThat(snapshot.admissionAllowed()).isFalse();
        assertThat(snapshot.stressedFraction()).isEqualTo(0.25);
    }

    @Test
    @DisplayName("Pressure updates via heartbeat are reflected in evaluation")
    void pressureUpdatesReflectedViaHeartbeat() {
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.NONE));
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w2", "host2", 8080, 9090, MemoryPressure.NONE));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        // Initially healthy
        assertThat(monitor.shouldAdmit()).isTrue();

        // Simulate heartbeat reporting CRITICAL on both workers
        workerManager.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity("w1", workerManager.getWorker("w1").workerExecutionIdentity().admissionDomainId(), workerManager.getWorker("w1").workerExecutionIdentity().incarnationId()),
                new WorkerManager.HeartbeatData(MemoryPressure.CRITICAL, 0.95, 10, 100));
        workerManager.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity("w2", workerManager.getWorker("w2").workerExecutionIdentity().admissionDomainId(), workerManager.getWorker("w2").workerExecutionIdentity().incarnationId()),
                new WorkerManager.HeartbeatData(MemoryPressure.CRITICAL, 0.90, 8, 200));

        // Now should reject
        assertThat(monitor.shouldAdmit()).isFalse();
        var snapshot = monitor.evaluate();
        assertThat(snapshot.criticalCount()).isEqualTo(2);
        assertThat(snapshot.rejectionReason()).contains("CRITICAL");
    }

    @Test
    @DisplayName("Single worker cluster: CRITICAL -> reject, then recover")
    void singleWorkerCriticalAndRecovery() {
        workerManager.registerWorker(
                TestWorkerInfoBuilder.withPressure("w1", "host1", 8080, 9090, MemoryPressure.NONE));

        var monitor = new ClusterResourcePressureMonitor(workerManager);

        assertThat(monitor.shouldAdmit()).isTrue();

        // Worker enters CRITICAL
        workerManager.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity("w1", workerManager.getWorker("w1").workerExecutionIdentity().admissionDomainId(), workerManager.getWorker("w1").workerExecutionIdentity().incarnationId()),
                new WorkerManager.HeartbeatData(MemoryPressure.CRITICAL, 0.99, 20, 50));
        assertThat(monitor.shouldAdmit()).isFalse();

        // Worker recovers
        workerManager.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity("w1", workerManager.getWorker("w1").workerExecutionIdentity().admissionDomainId(), workerManager.getWorker("w1").workerExecutionIdentity().incarnationId()),
                new WorkerManager.HeartbeatData(MemoryPressure.LOW, 0.3, 2, 500_000));
        assertThat(monitor.shouldAdmit()).isTrue();
    }

    @Test
    @DisplayName("Constructor rejects null workerManager")
    void rejectsNullWorkerManager() {
        assertThatThrownBy(() -> new ClusterResourcePressureMonitor(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerManager");
    }

    @Test
    @DisplayName("Constructor rejects invalid threshold")
    void rejectsInvalidThreshold() {
        assertThatThrownBy(() -> new ClusterResourcePressureMonitor(workerManager, -0.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pressureThreshold");

        assertThatThrownBy(() -> new ClusterResourcePressureMonitor(workerManager, 1.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pressureThreshold");
    }

    @Test
    @DisplayName("getPressureThreshold returns configured value")
    void getPressureThresholdReturnsConfiguredValue() {
        var monitor = new ClusterResourcePressureMonitor(workerManager, 0.75);
        assertThat(monitor.getPressureThreshold()).isEqualTo(0.75);
    }
}
