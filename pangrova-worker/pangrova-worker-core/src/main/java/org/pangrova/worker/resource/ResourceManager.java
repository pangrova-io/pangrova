package org.pangrova.worker.resource;

import org.pangrova.common.value.MemoryPressure;
import org.pangrova.worker.resource.memory.CriticalPressureHandler;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;

/**
 * Unified entry point for resource management, delegating to specialized managers.
 *
 * <p>When memory pressure reaches CRITICAL, the manager first attempts spill,
 * then invokes {@link CriticalPressureHandler} to select and terminate the
 * largest query — ensuring the node does not OOM.</p>
 */
public class ResourceManager {

    private final MemoryArbitrator memoryArbitrator;
    private final ThreadBudgetTracker threadPoolManager;
    private final SpillManager spillManager;
    private final CriticalPressureHandler criticalPressureHandler;

    /**
     * Creates a ResourceManager without CRITICAL kill capability (legacy behavior).
     */
    public ResourceManager(MemoryArbitrator memoryArbitrator,
                           ThreadBudgetTracker threadPoolManager,
                           SpillManager spillManager) {
        this(memoryArbitrator, threadPoolManager, spillManager, null);
    }

    /**
     * Creates a ResourceManager with full CRITICAL pressure handling.
     *
     * @param memoryArbitrator         global memory arbitrator
     * @param threadPoolManager        thread budget tracker
     * @param spillManager             spill manager for reclaiming memory via disk
     * @param criticalPressureHandler  handler for CRITICAL pressure kill path (null disables)
     */
    public ResourceManager(MemoryArbitrator memoryArbitrator,
                           ThreadBudgetTracker threadPoolManager,
                           SpillManager spillManager,
                           CriticalPressureHandler criticalPressureHandler) {
        this.memoryArbitrator = memoryArbitrator;
        this.threadPoolManager = threadPoolManager;
        this.spillManager = spillManager;
        this.criticalPressureHandler = criticalPressureHandler;
    }

    public MemoryArbitrator getMemoryArbitrator() {
        return memoryArbitrator;
    }

    public ThreadBudgetTracker getThreadPoolManager() {
        return threadPoolManager;
    }

    public SpillManager getSpillManager() {
        return spillManager;
    }

    public MemoryPressure getOverallPressure() {
        return memoryArbitrator.getPressure();
    }

    /**
     * Responds to memory pressure by triggering spill and, at CRITICAL level,
     * killing the largest query to prevent OOM.
     *
     * <p>At CRITICAL pressure, the sequence is:
     * <ol>
     *   <li>Attempt spill to reclaim {@code targetBytes}</li>
     *   <li>If spill alone is insufficient and a {@link CriticalPressureHandler}
     *       is configured, select and terminate the largest query</li>
     * </ol>
     *
     * @param targetBytes bytes to reclaim
     * @return bytes actually reclaimed via spill (kill reclamation is asynchronous)
     */
    public long handleMemoryPressure(long targetBytes) {
        MemoryPressure pressure = memoryArbitrator.getPressure();
        if (pressure == MemoryPressure.NONE) {
            return 0;
        }

        long reclaimed = spillManager.handleMemoryPressure(pressure, targetBytes);

        if (pressure == MemoryPressure.CRITICAL && reclaimed < targetBytes
                && criticalPressureHandler != null) {
            criticalPressureHandler.killLargestQuery();
        }

        return reclaimed;
    }
}
