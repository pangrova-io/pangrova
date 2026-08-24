package org.pangrova.resource.memory;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-node memory pool. Reserves its quota from the parent
 * {@link RootMemoryPool}.
 */
@ThreadSafe
public final class NodeMemoryPool extends MemoryPool {

    private final RootMemoryPool parent;
    private final ConcurrentHashMap<String, QueryMemoryPool> activeQueryPools = new ConcurrentHashMap<>();

    NodeMemoryPool(String name, long maxBytes, RootMemoryPool parent) {
        super(name, maxBytes);
        this.parent = parent;
    }

    /**
     * Creates a child {@link QueryMemoryPool} whose quota is reserved
     * from this node pool.
     */
    public QueryMemoryPool createQueryPool(String queryId, long queryBytes) {
        return createQueryPool(queryId, queryBytes, "default");
    }

    /**
     * Creates a child {@link QueryMemoryPool} with tenant identity.
     */
    public QueryMemoryPool createQueryPool(String queryId, long queryBytes, String tenantId) {
        long reserved = tryReserve(queryBytes);
        if (reserved < queryBytes) {
            if (reserved > 0) {
                free(reserved);
            }
            throw new InsufficientMemoryException(
                    queryBytes,
                    new InsufficientMemoryException.PoolSnapshot(
                            getName(), getFreeBytes(), getUsedBytes(), getMaxBytes()));
        }
        QueryMemoryPool pool = new QueryMemoryPool(queryId, queryBytes, this, tenantId);
        activeQueryPools.put(queryId, pool);
        return pool;
    }

    /**
     * Removes a query pool from active tracking (called when query finishes).
     */
    public void removeQueryPool(String queryId) {
        activeQueryPools.remove(queryId);
    }

    /**
     * Returns a snapshot of all currently active query pools on this node.
     * Used by {@link CriticalPressureHandler} for victim selection.
     */
    public List<QueryMemoryPool> getActiveQueryPools() {
        return List.copyOf(activeQueryPools.values());
    }

    public RootMemoryPool getParent() {
        return parent;
    }
}
