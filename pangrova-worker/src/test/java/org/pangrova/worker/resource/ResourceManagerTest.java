package org.pangrova.worker.resource;

import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.worker.resource.memory.CriticalPressureHandler;
import org.pangrova.worker.resource.memory.LargestQueryKiller;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.memory.QueryMemoryPool;
import org.pangrova.worker.resource.memory.RootMemoryPool;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.spill.DiskSpillableBuffer;
import org.pangrova.worker.resource.spill.SpillManager.SpillCandidate;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ResourceManager}.
 *
 * <p><b>Validates: Requirements 11.1</b></p>
 */
class ResourceManagerTest {

    @TempDir
    Path spillDirectory;

    private final java.util.List<DiskSpillableBuffer> actualBuffers = new java.util.ArrayList<>();

    private SpillCandidate actualCandidate(String id, int priority, int bytes) {
        DiskSpillableBuffer buffer = new DiskSpillableBuffer(id, priority, spillDirectory);
        buffer.addBatch(new byte[bytes]);
        actualBuffers.add(buffer);
        return buffer.toSpillCandidate();
    }

    @AfterEach
    void closeActualBuffers() {
        for (DiskSpillableBuffer buffer : actualBuffers) {
            buffer.close();
        }
    }


    @Test
    void delegatesToMemoryArbitrator() {
        var arbitrator = new MemoryArbitrator(1000, 100);
        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var spill = new SpillManager();
        var manager = new ResourceManager(arbitrator, threadPool, spill);

        assertThat(manager.getMemoryArbitrator()).isSameAs(arbitrator);
        assertThat(manager.getThreadPoolManager()).isSameAs(threadPool);
        assertThat(manager.getSpillManager()).isSameAs(spill);
    }

    @Test
    void overallPressureReflectsMemoryArbitrator() {
        var arbitrator = new MemoryArbitrator(1000, 0);
        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var spill = new SpillManager();
        var manager = new ResourceManager(arbitrator, threadPool, spill);

        assertThat(manager.getOverallPressure()).isEqualTo(MemoryPressure.NONE);
    }

    @Test
    void criticalPressureTriggersKill() {
        // Budget = 1000, reserve 960 → ratio = 0.96 → CRITICAL
        var arbitrator = new MemoryArbitrator(1000, 0);
        arbitrator.requestMemory("q-big", 960);

        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var spill = new SpillManager(); // no candidates → reclaims 0

        // Set up query pools for the killer to choose from
        var root = new RootMemoryPool("root", 10_000);
        NodeMemoryPool node = root.createNodePool("node-1", 10_000);
        QueryMemoryPool q1 = node.createQueryPool("q-small", 3000, "tenant-a");
        q1.tryReserve(500);
        QueryMemoryPool q2 = node.createQueryPool("q-big", 5000, "tenant-b");
        q2.tryReserve(3000);

        AtomicReference<QueryMemoryPool> killed = new AtomicReference<>();
        var handler = new CriticalPressureHandler(
                new LargestQueryKiller(),
                () -> List.of(q1, q2),
                killed::set);

        var manager = new ResourceManager(arbitrator, threadPool, spill, handler);

        long reclaimed = manager.handleMemoryPressure(100);

        // Spill reclaimed 0 (no candidates), but kill was triggered
        assertThat(reclaimed).isZero();
        assertThat(killed.get()).isSameAs(q2);
    }

    @Test
    void nonCriticalPressureDoesNotTriggerKill() {
        // Budget = 1000, reserve 860 → ratio = 0.86 → HIGH (not CRITICAL)
        var arbitrator = new MemoryArbitrator(1000, 0);
        arbitrator.requestMemory("q-1", 860);

        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var spill = new SpillManager();

        List<QueryMemoryPool> killLog = new ArrayList<>();
        var handler = new CriticalPressureHandler(
                new LargestQueryKiller(),
                List::of,
                killLog::add);

        var manager = new ResourceManager(arbitrator, threadPool, spill, handler);

        manager.handleMemoryPressure(100);

        assertThat(killLog).isEmpty();
    }

    @Test
    void criticalPressureWithSufficientSpillDoesNotKill() {
        // Budget = 1000, reserve 960 → CRITICAL
        var arbitrator = new MemoryArbitrator(1000, 0);
        arbitrator.requestMemory("q-1", 960);

        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var spill = new SpillManager();
        // Register a spill candidate that can reclaim enough
        spill.registerCandidate(actualCandidate("scan-buf-1", 1, 200));

        List<QueryMemoryPool> killLog = new ArrayList<>();
        var handler = new CriticalPressureHandler(
                new LargestQueryKiller(),
                List::of,
                killLog::add);

        var manager = new ResourceManager(arbitrator, threadPool, spill, handler);

        long reclaimed = manager.handleMemoryPressure(100);

        // Spill reclaimed enough (200 >= 100), so no kill
        assertThat(reclaimed).isEqualTo(200);
        assertThat(killLog).isEmpty();
    }

    @Test
    void nullHandlerDoesNotCrashAtCriticalPressure() {
        // Budget = 1000, reserve 960 → CRITICAL
        var arbitrator = new MemoryArbitrator(1000, 0);
        arbitrator.requestMemory("q-1", 960);

        var threadPool = new ThreadBudgetTracker(4, 2, 1);
        var spill = new SpillManager();

        // No handler (legacy 3-arg constructor)
        var manager = new ResourceManager(arbitrator, threadPool, spill);

        // Should not throw
        long reclaimed = manager.handleMemoryPressure(100);
        assertThat(reclaimed).isZero();
    }
}
