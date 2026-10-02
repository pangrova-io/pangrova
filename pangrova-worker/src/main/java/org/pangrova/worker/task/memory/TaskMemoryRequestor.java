package org.pangrova.worker.task.memory;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.worker.task.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Handles memory allocation and release for worker tasks via MemoryArbitrator,
 * with hierarchical budget enforcement through {@link NodeMemoryPool}.
 *
 * <p>Descriptor-only tasks reserve their memory through both budgets. For
 * canonical tasks, Native memory is a growth limit; the Native allocation
 * callback accounts for actual bytes through both budgets. Serialized buffers
 * are still exact up-front reservations.</p>
 */
public final class TaskMemoryRequestor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMemoryRequestor.class);

    private static final double BUFFER_MEMORY_FRACTION = 0.30;
    private static final long DEFAULT_BUFFER_MEMORY = 16L * 1024 * 1024;

    private final MemoryArbitrator arbitrator;
    private final double minAllocationRatio;
    private volatile org.pangrova.resource.ResourceManager resourceManager;

    /** Hierarchical node memory pool for per-node budget enforcement. Nullable. */
    private volatile NodeMemoryPool nodeMemoryPool;

    public TaskMemoryRequestor(MemoryArbitrator arbitrator, double minAllocationRatio) {
        this.arbitrator = arbitrator;
        this.minAllocationRatio = minAllocationRatio;
    }

    /** Sets the resource manager for spill-on-pressure integration. */
    public void setResourceManager(org.pangrova.resource.ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    /** Sets the node memory pool for hierarchical budget enforcement. */
    public void setNodeMemoryPool(NodeMemoryPool pool) {
        this.nodeMemoryPool = pool;
    }

    /** Reserves descriptor-only memory; canonical Native capacity grows on demand. */
    public void requestMemoryForTask(TaskInfo info) {
        long requested = resolveMemoryLimit(info);
        if (requested <= 0) {
            return;
        }

        // A canonical Native grant is a per-task *maximum*, not resident memory.
        // NativeMemoryCoordinator charges actual Velox growth through this same
        // arbitrator. Pre-reserving the maximum here charges every Native byte
        // twice and makes a single task monopolize the node before it allocates.
        // Serialized transport buffers remain exact admission reservations.
        if (info.request() != null) {
            return;
        }

        // Enforce node-level memory budget: reserve from NodeMemoryPool before
        // the arbitrator allocation to prevent queries from collectively exceeding
        // the node's physical memory capacity.
        NodeMemoryPool nodePool = this.nodeMemoryPool;
        long nodeReserved = reserveFromNodePool(nodePool, info, requested);

        long allocated;
        try {
            allocated = allocateWithSpill(info, requested);
        } catch (Exception e) {
            releaseNodePool(nodePool, nodeReserved);
            throw e;
        }

        info.setAllocatedMemory(allocated);

        // Reconcile node pool reservation with actual arbitrator allocation:
        // if the arbitrator gave less than requested, release the excess from
        // the node pool so the budget stays accurate.
        if (allocated < nodeReserved && nodePool != null) {
            nodePool.free(nodeReserved - allocated);
        }

        if (allocated < requested) {
            logPartialAllocation(info, allocated, requested);
            rejectIfBelowThreshold(info, allocated, requested);
        }
        LOG.debug("event=task_memory_allocated taskId={} allocated={} requested={} nodePoolEnforced={}",
                info.taskId(), allocated, requested, nodePool != null);
    }

    private long allocateWithSpill(TaskInfo info, long requested) {
        long allocated = arbitrator.requestMemory(info.queryId(), requested);
        if (allocated < requested && resourceManager != null) {
            long reclaimed = resourceManager.handleMemoryPressure(requested - allocated);
            if (reclaimed > 0) {
                long extra = arbitrator.requestMemory(info.queryId(), requested - allocated);
                allocated += extra;
                LOG.info("event=spill_reclaim_retry taskId={} reclaimed={} extraAllocated={}",
                        info.taskId(), reclaimed, extra);
            }
        }
        return allocated;
    }

    /** Reserves the exact canonical transport-buffer grant through both memory budgets. */
    public void requestBufferMemory(TaskInfo info) {
        boolean shuffleTask = isShuffleTask(info);
        boolean canonicalTask = info.request() != null;
        long requested = canonicalTask
                ? info.request().taskRequest().getResourceGrant().getShuffleBufferBytes()
                : resolveBufferMemoryLimit(info);
        if (shuffleTask && requested <= 0) {
            throw new PangrovaException(PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED,
                    "Shuffle task requires a positive serialized-buffer grant: " + info.taskId());
        }
        if (requested <= 0) {
            return;
        }

        // Enforce node-level budget for buffer memory
        NodeMemoryPool nodePool = this.nodeMemoryPool;
        OptionalLong nodeReservation = reserveBufferFromNodePool(nodePool, info, requested, canonicalTask);
        if (nodeReservation.isEmpty()) {
            return;
        }
        long nodeReserved = nodeReservation.getAsLong();

        String bufferKey = "buffer:" + info.taskId();
        long allocated;
        try {
            allocated = arbitrator.requestMemory(bufferKey, requested);
        } catch (Exception e) {
            releaseNodePool(nodePool, nodeReserved);
            throw e;
        }

        if (canonicalTask && allocated != requested) {
            if (allocated > 0) {
                arbitrator.release(bufferKey, allocated);
            }
            releaseNodePool(nodePool, nodeReserved);
            throw new PangrovaException(PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED,
                    "Canonical task requires its complete buffer grant: " + info.taskId());
        }

        // Reconcile node pool with actual allocation
        if (allocated < nodeReserved && nodePool != null) {
            nodePool.free(nodeReserved - allocated);
        }

        info.setAllocatedBufferMemory(allocated);
        LOG.debug("event=buffer_memory_allocated taskId={} allocated={} requested={}",
                info.taskId(), allocated, requested);
    }

    private static OptionalLong reserveBufferFromNodePool(
            NodeMemoryPool nodePool, TaskInfo info, long requested, boolean canonicalTask) {
        if (nodePool == null) {
            return OptionalLong.of(0);
        }
        long reserved = nodePool.tryReserve(requested);
        if (reserved == requested) {
            return OptionalLong.of(reserved);
        }
        releaseNodePool(nodePool, reserved);
        if (canonicalTask) {
            throw new PangrovaException(PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED,
                    "Canonical task has insufficient node buffer budget: " + info.taskId());
        }
        // Non-shuffle buffer memory is optional.
        LOG.warn("event=buffer_memory_node_pool_insufficient taskId={} requested={} available={}",
                info.taskId(), requested, nodePool.getFreeBytes());
        return OptionalLong.empty();
    }

    private static boolean isShuffleTask(TaskInfo info) {
        return info.request() != null
                && info.request().taskRequest().hasExecutionOptions()
                && info.request().taskRequest().getExecutionOptions().hasShuffle();
    }

    /** Releases the task memory previously reserved for the given task. */
    public void releaseTaskMemory(TaskInfo info) {
        long allocated = info.allocatedMemory();
        if (allocated > 0) {
            info.setAllocatedMemory(0); // Claim-and-reset for idempotent release
            arbitrator.release(info.queryId(), allocated);
            // Release from node pool to make budget available for other tasks
            releaseNodePool(this.nodeMemoryPool, allocated);
            LOG.debug("event=task_memory_released taskId={} bytes={}", info.taskId(), allocated);
        }
    }

    /** Releases the buffer memory previously reserved for the given task. */
    public void releaseBufferMemory(TaskInfo info) {
        long bufferMemory = info.allocatedBufferMemory();
        if (bufferMemory > 0) {
            info.setAllocatedBufferMemory(0); // Claim-and-reset for idempotent release
            arbitrator.release("buffer:" + info.taskId(), bufferMemory);
            // Release from node pool to make budget available for other tasks
            releaseNodePool(this.nodeMemoryPool, bufferMemory);
            LOG.debug("event=buffer_memory_released taskId={} bytes={}", info.taskId(), bufferMemory);
        }
    }

    private void rejectIfBelowThreshold(
            TaskInfo info, long allocated, long requested) {
        if (minAllocationRatio <= 0.0) {
            return;
        }
        double ratio = (double) allocated / requested;
        if (ratio < minAllocationRatio) {
            arbitrator.release(info.queryId(), allocated);
            info.setAllocatedMemory(0);
            // Release the node pool reservation that was reconciled to 'allocated'
            releaseNodePool(this.nodeMemoryPool, allocated);
            throw new PangrovaException(
                    PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED,
                    String.format("Task %s rejected: allocation ratio %.2f below minimum %.2f",
                            info.taskId(), ratio, minAllocationRatio));
        }
    }

    /**
     * Reserves memory from the node pool for hierarchical budget enforcement.
     * If the node pool cannot satisfy the full request, throws an exception
     * to reject the task before any arbitrator allocation occurs.
     *
     * @return bytes reserved from the node pool (0 if no node pool configured)
     */
    private long reserveFromNodePool(NodeMemoryPool nodePool, TaskInfo info, long requested) {
        if (nodePool == null) {
            return 0;
        }
        long reserved = nodePool.tryReserve(requested);
        if (reserved < requested) {
            if (reserved > 0) {
                nodePool.free(reserved);
            }
            throw new PangrovaException(
                    PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED,
                    String.format("Task %s rejected: insufficient node memory budget. "
                            + "requested=%d, available=%d, used=%d, max=%d",
                            info.taskId(), requested, nodePool.getFreeBytes(),
                            nodePool.getUsedBytes(), nodePool.getMaxBytes()));
        }
        return reserved;
    }

    /**
     * Releases memory back to the node pool. Safe to call with null pool or zero bytes.
     */
    private static void releaseNodePool(NodeMemoryPool nodePool, long bytes) {
        if (nodePool != null && bytes > 0) {
            nodePool.free(bytes);
        }
    }

    private long resolveBufferMemoryLimit(TaskInfo info) {
        long taskMemory = resolveMemoryLimit(info);
        if (taskMemory > 0) {
            return (long) (taskMemory * BUFFER_MEMORY_FRACTION);
        }
        return DEFAULT_BUFFER_MEMORY;
    }

    /** Resolves the canonical grant first; descriptor-only tasks are test wiring. */
    public static long resolveMemoryLimit(TaskInfo info) {
        if (info.request() != null) {
            return info.request().taskRequest().getResourceGrant().getNativeMemoryBytes();
        }
        TaskDescriptor desc = info.descriptor();
        if (desc == null) {
            return 0;
        }
        TaskResourceLimit limit = desc.resourceLimit();
        return (limit != null) ? limit.maxMemoryBytes() : 0;
    }

    private void logPartialAllocation(
            TaskInfo info, long allocated, long requested) {
        LOG.warn("event=partial_memory_allocation taskId={} stageId={} taskType={} allocated={} requested={}",
                info.taskId(), info.descriptor().stageId(),
                info.descriptor().taskType(), allocated, requested);
    }
}
