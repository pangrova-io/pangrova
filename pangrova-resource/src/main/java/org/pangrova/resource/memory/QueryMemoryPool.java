package org.pangrova.resource.memory;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Per-query memory pool. Reserves its quota from the parent
 * {@link NodeMemoryPool}.
 */
@ThreadSafe
public final class QueryMemoryPool extends MemoryPool {

    private final NodeMemoryPool parent;
    private final String tenantId;

    QueryMemoryPool(String name, long maxBytes, NodeMemoryPool parent) {
        this(name, maxBytes, parent, "default");
    }

    QueryMemoryPool(String name, long maxBytes, NodeMemoryPool parent, String tenantId) {
        super(name, maxBytes);
        this.parent = parent;
        this.tenantId = tenantId != null ? tenantId : "default";
    }

    /**
     * Creates a child {@link TaskMemoryPool} whose quota is reserved
     * from this query pool and propagated to the parent node pool.
     */
    public TaskMemoryPool createTaskPool(String taskId, long taskBytes) {
        long reserved = tryReserve(taskBytes);
        if (reserved < taskBytes) {
            if (reserved > 0) {
                free(reserved);
            }
            throw new InsufficientMemoryException(
                    taskBytes,
                    new InsufficientMemoryException.PoolSnapshot(
                            getName(), getFreeBytes(), getUsedBytes(), getMaxBytes()));
        }
        // Propagate reservation to the parent node pool to enforce node-level cap.
        long parentReserved = parent.tryReserve(taskBytes);
        if (parentReserved < taskBytes) {
            if (parentReserved > 0) {
                parent.free(parentReserved);
            }
            // Roll back query-level reservation.
            free(reserved);
            throw new InsufficientMemoryException(
                    taskBytes,
                    new InsufficientMemoryException.PoolSnapshot(
                            parent.getName(), parent.getFreeBytes(),
                            parent.getUsedBytes(), parent.getMaxBytes()));
        }
        return new TaskMemoryPool(taskId, taskBytes, this);
    }

    public NodeMemoryPool getParent() {
        return parent;
    }

    public String getTenantId() {
        return tenantId;
    }
}
