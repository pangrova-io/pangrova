package org.pangrova.worker.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies observability helpers: CPU usage reader and spill disk monitor.
 */
class UnifiedBudgetObservabilityTest {


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

}
