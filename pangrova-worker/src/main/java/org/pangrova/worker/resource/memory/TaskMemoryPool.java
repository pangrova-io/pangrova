package org.pangrova.worker.resource.memory;

import org.pangrova.worker.exception.InsufficientMemoryException;

import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Per-task memory pool. Reserves its quota from the parent
 * {@link QueryMemoryPool}.
 */
@ThreadSafe
public final class TaskMemoryPool extends MemoryPool {

    private final QueryMemoryPool parent;

    TaskMemoryPool(String name, long maxBytes, QueryMemoryPool parent) {
        super(name, maxBytes);
        this.parent = parent;
    }

    /**
     * Creates a child {@link OperatorMemoryPool} whose quota is reserved
     * from this task pool.
     */
    public OperatorMemoryPool createOperatorPool(String operatorName, long operatorBytes) {
        long reserved = tryReserve(operatorBytes);
        if (reserved < operatorBytes) {
            if (reserved > 0) {
                free(reserved);
            }
            throw new InsufficientMemoryException(
                    operatorBytes,
                    new InsufficientMemoryException.PoolSnapshot(
                            getName(), getFreeBytes(), getUsedBytes(), getMaxBytes()));
        }
        return new OperatorMemoryPool(operatorName, operatorBytes, this);
    }

    public QueryMemoryPool getParent() {
        return parent;
    }
}
