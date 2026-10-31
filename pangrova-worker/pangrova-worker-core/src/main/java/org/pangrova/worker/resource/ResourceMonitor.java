package org.pangrova.worker.resource;

import org.pangrova.common.value.MemoryPressure;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;

/**
 * Provides real-time resource monitoring snapshots.
 */
public class ResourceMonitor {

    private final MemoryArbitrator memoryArbitrator;
    private final ThreadBudgetTracker threadPoolManager;

    public record ResourceSnapshot(
            MemoryPressure pressure,
            long usedMemory,
            long availableMemory,
            int allocatedThreads,
            int availableThreads) {}

    public ResourceMonitor(MemoryArbitrator memoryArbitrator, ThreadBudgetTracker threadPoolManager) {
        this.memoryArbitrator = memoryArbitrator;
        this.threadPoolManager = threadPoolManager;
    }

    public ResourceSnapshot snapshot() {
        return new ResourceSnapshot(
                memoryArbitrator.getPressure(),
                memoryArbitrator.getUsedMemory(),
                memoryArbitrator.getAvailableMemory(),
                threadPoolManager.getAllocatedThreads(),
                threadPoolManager.getAvailableThreads());
    }

    public boolean isHealthy() {
        return memoryArbitrator.getPressure() != MemoryPressure.CRITICAL;
    }
}
