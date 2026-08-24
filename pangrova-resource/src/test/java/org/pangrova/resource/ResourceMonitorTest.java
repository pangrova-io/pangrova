package org.pangrova.resource;

import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ResourceMonitor}.
 *
 * <p><b>Validates: Requirements 14.7</b></p>
 */
class ResourceMonitorTest {

    @Test
    void snapshotReflectsCurrentState() {
        var arbitrator = new MemoryArbitrator(1000, 100);
        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var monitor = new ResourceMonitor(arbitrator, threadPool);

        arbitrator.requestMemory("q1", 200);
        threadPool.allocateThreads(3);

        var snapshot = monitor.snapshot();
        assertThat(snapshot.pressure()).isEqualTo(MemoryPressure.NONE);
        assertThat(snapshot.usedMemory()).isEqualTo(200);
        assertThat(snapshot.availableMemory()).isEqualTo(700);
        assertThat(snapshot.allocatedThreads()).isEqualTo(3);
        assertThat(snapshot.availableThreads()).isEqualTo(4);
    }

    @Test
    void isHealthyReturnsTrueWhenNotCritical() {
        var arbitrator = new MemoryArbitrator(1000, 0);
        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var monitor = new ResourceMonitor(arbitrator, threadPool);

        assertThat(monitor.isHealthy()).isTrue();
    }

    @Test
    void isHealthyReturnsFalseWhenCritical() {
        var arbitrator = new MemoryArbitrator(1000, 0);
        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var monitor = new ResourceMonitor(arbitrator, threadPool);

        // Allocate 95%+ to trigger CRITICAL
        arbitrator.requestMemory("q1", 960);

        assertThat(monitor.isHealthy()).isFalse();
    }
}
