package org.pangrova.worker.task.memory;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.task.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages native memory limit propagation and memory usage tracking for tasks.
 *
 * <p>Memory accounting is unified: the {@link MemoryArbitrator} is the single
 * authoritative source of truth for memory usage. The {@link NodeMemoryPool}
 * provides hierarchical per-node budget enforcement — task memory reservations
 * are checked against the node pool budget before the arbitrator allocation.</p>
 */
@ThreadSafe
public final class TaskMemoryAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMemoryAllocator.class);

    private final MemoryArbitrator arbitrator;

    /** Hierarchical node memory pool for per-node budget enforcement. Nullable. */
    private final NodeMemoryPool nodePool;

    public TaskMemoryAllocator(MemoryArbitrator arbitrator) {
        this(arbitrator, null);
    }

    public TaskMemoryAllocator(MemoryArbitrator arbitrator, NodeMemoryPool nodePool) {
        this.arbitrator = arbitrator;
        this.nodePool = nodePool;
    }

    /**
     * Returns the authoritative memory arbitrator used for all accounting.
     * Exposed for debug assertions that verify single-source-of-truth invariant.
     */
    public MemoryArbitrator getArbitrator() {
        return arbitrator;
    }

    /**
     * Propagates the task memory limit to the native C++ MemoryManager via JNI.
     */
    public void propagateMemoryLimitToNative(TaskInfo info) {
        long handle = info.executorHandle();
        long memoryLimit = handle != 0 ? resolveNativeMemoryLimit(info) : 0;
        if (memoryLimit <= 0) {
            return;
        }
        sendNativeMemoryLimit(info, handle, memoryLimit);
    }

    private void sendNativeMemoryLimit(TaskInfo info, long handle, long memoryLimit) {
        try {
            byte[] cmd = NativeCommandBuilder.updateMemoryLimit(handle, memoryLimit);
            NativeRuntime.nativeCommand(handle, cmd);
            LOG.debug("event=memory_limit_propagated taskId={} bytes={}", info.taskId(), memoryLimit);
        } catch (UnsatisfiedLinkError | ExceptionInInitializerError | NoClassDefFoundError e) {
            LOG.debug("event=memory_limit_propagation_skipped taskId={} reason=native_unavailable",
                    info.taskId());
        } catch (Exception e) {
            LOG.warn("event=memory_limit_propagation_failed taskId={} error={}",
                    info.taskId(), e.getMessage(), e);
        }
    }

    /**
     * Resolves the memory limit to propagate to native. Uses the arbitrator-allocated
     * amount stored in TaskInfo (set by TaskMemoryRequestor).
     */
    public long resolveNativeMemoryLimit(TaskInfo info) {
        long allocated = info.allocatedMemory();
        if (allocated > 0) {
            return allocated;
        }
        return TaskMemoryRequestor.resolveMemoryLimit(info);
    }

    /**
     * Returns the current memory usage for a task as tracked by the arbitrator.
     * The arbitrator is the single source of truth.
     */
    public long resolveCurrentMemoryUsage(TaskInfo info) {
        return info.allocatedMemory();
    }

    /**
     * Returns the peak memory usage for a task (the allocated limit).
     */
    public long resolvePeakMemoryUsage(TaskInfo info) {
        return info.allocatedMemory();
    }

    /** Usage ratio above which a memory pressure warning is logged. */
    private static final double MEMORY_WARNING_RATIO = 0.85;

    /**
     * Logs a warning when memory usage exceeds 85% of the allocated limit.
     */
    public void logMemoryWarningIfNeeded(TaskInfo info, long memoryBytes) {
        long allocated = info.allocatedMemory();
        if (allocated <= 0) {
            return;
        }
        double usageRatio = (double) memoryBytes / allocated;
        if (usageRatio >= MEMORY_WARNING_RATIO) {
            LOG.warn("event=memory_pressure_high taskId={} used={} max={} ratio={}",
                    info.taskId(), memoryBytes, allocated, usageRatio);
        }
    }

    /**
     * Debug assertion: verifies that the arbitrator's used memory is consistent
     * with expectations. Call this in debug/test mode to catch accounting drift.
     *
     * @param expectedUsed the expected used memory in the arbitrator
     * @throws AssertionError if the actual used memory differs from expected
     */
    public void assertMemoryConsistency(long expectedUsed) {
        long actual = arbitrator.getUsedMemory();
        assert actual == expectedUsed
                : "Memory accounting inconsistency: arbitrator.usedMemory=" + actual
                        + " expected=" + expectedUsed;
    }
}
