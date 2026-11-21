package org.pangrova.worker.resource.memory;

import org.pangrova.worker.exception.InsufficientMemoryException;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Top-level pool representing the per-node memory budget.
 * Acts as the root of the memory hierarchy:
 * Root → Node → Query → Task → Operator.
 *
 * <p>Despite being the root of the hierarchy, this pool is scoped to a single
 * worker process — it does not represent cluster-wide shared memory.</p>
 */
@ThreadSafe
public final class RootMemoryPool extends MemoryPool {

    public RootMemoryPool(String name, long maxBytes) {
        super(name, maxBytes);
    }

    /**
     * Creates a child {@link NodeMemoryPool} whose quota is reserved
     * from this root pool.
     *
     * @param nodeName  human-readable node identifier
     * @param nodeBytes maximum bytes for the node pool
     * @return a new NodeMemoryPool, or throws if reservation fails
     * @throws InsufficientMemoryException if the root pool cannot
     *         satisfy the full reservation
     */
    public NodeMemoryPool createNodePool(String nodeName, long nodeBytes) {
        long reserved = tryReserve(nodeBytes);
        if (reserved < nodeBytes) {
            if (reserved > 0) {
                free(reserved);
            }
            throw new InsufficientMemoryException(
                    nodeBytes,
                    new InsufficientMemoryException.PoolSnapshot(
                            getName(), getFreeBytes(), getUsedBytes(), getMaxBytes()));
        }
        return new NodeMemoryPool(nodeName, nodeBytes, this);
    }
}
