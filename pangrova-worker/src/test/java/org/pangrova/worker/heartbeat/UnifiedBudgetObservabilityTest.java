package org.pangrova.worker.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies observability helpers: CPU usage reader and spill disk monitor.
 */
class UnifiedBudgetObservabilityTest {
    private static final org.pangrova.protocol.execution.identity.WorkerExecutionIdentity HEARTBEAT_IDENTITY = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");


    @Test
    void cpuUsageReader_returnsNonNegativeValue() {
        double cpu = CpuUsageReader.getProcessCpuLoad();
        // CPU load should be in [0.0, 1.0]
        assertThat(cpu).isBetween(0.0, 1.0);
    }

    @Test
    void cpuUsageReader_nonZeroUnderLoad() {
        // Generate CPU load to ensure the reading is non-zero
        long start = System.nanoTime();
        double sum = 0;
        // Spin for ~100ms to generate measurable CPU activity
        while (System.nanoTime() - start < 100_000_000L) {
            sum += Math.sqrt(System.nanoTime());
        }
        // Prevent dead code elimination
        assertThat(sum).isNotZero();

        // After CPU work, the process CPU load should be reportable.
        // Note: on the very first call after JVM start, the JMX bean may still
        // return 0.0 (it needs one sampling interval). We retry a few times.
        double cpu = 0.0;
        for (int i = 0; i < 10; i++) {
            cpu = CpuUsageReader.getProcessCpuLoad();
            if (cpu > 0.0) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // The CPU load should be > 0 after doing work (with retries for sampling lag)
        assertThat(cpu).as("CPU utilization should be non-zero after sustained computation")
                .isGreaterThan(0.0);
    }

    @Test
    void spillDiskMonitor_reportsAvailableBytes(@TempDir Path tempDir) {
        SpillDiskMonitor monitor = new SpillDiskMonitor(tempDir.toString());
        long available = monitor.getAvailableBytes();
        // A temp directory on any filesystem should have some usable space
        assertThat(available).as("spill_disk_available_bytes on existing directory")
                .isGreaterThan(0);
        assertThat(monitor.isConfigured()).isTrue();
    }

    @Test
    void spillDiskMonitor_returnsZeroWhenNotConfigured() {
        SpillDiskMonitor monitor = new SpillDiskMonitor(null);
        assertThat(monitor.getAvailableBytes()).isZero();
        assertThat(monitor.isConfigured()).isFalse();
    }

    @Test
    void spillDiskMonitor_returnsZeroForNonexistentDir() {
        SpillDiskMonitor monitor = new SpillDiskMonitor("/nonexistent/path/xyz123");
        long available = monitor.getAvailableBytes();
        // Non-existent directory: getUsableSpace() returns 0
        assertThat(available).isZero();
    }

    @Test
    void heartbeatPayload_includesObservabilityFields() {
        HeartbeatPayload payload = new HeartbeatPayload(
                "w-obs-test", "NONE", 0.42, 2, 1024L,
                512L, 2048L, HEARTBEAT_IDENTITY);

        assertThat(payload.cpuUtilization()).isEqualTo(0.42);
        assertThat(payload.availableMemoryBytes()).isEqualTo(1024L);
        assertThat(payload.nativeUsedBytes()).isEqualTo(512L);
        assertThat(payload.nativeCapacityBytes()).isEqualTo(2048L);
    }

    @Test
    void heartbeatPayload_5argConstructorSetsDefaults() {
        HeartbeatPayload payload = new HeartbeatPayload("w-compat", "NONE", 0.1, 0, 1024L, HEARTBEAT_IDENTITY);
        assertThat(payload.nativeUsedBytes()).isZero();
        assertThat(payload.nativeCapacityBytes()).isZero();
    }
}
