package org.pangrova.security.tenant;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.security.tenant.ResourcePool;
import org.pangrova.spi.security.tenant.Tenant;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Manages resource pool assignment, availability checks, and resource acquisition/release.
 *
 * <p>Concurrency strategy: ConcurrentHashMap + AtomicInteger/AtomicLong for lock-free state.</p>
 */
@ThreadSafe
public class ResourcePoolManager {

    private final Map<String, ResourcePool> pools;
    private final Map<String, PoolState> poolStates = new ConcurrentHashMap<>();
    private final String defaultPoolId;

    /**
     * Creates a resource pool manager.
     *
     * @param pools the available resource pools
     * @param defaultPoolId the fallback pool identifier
     * @throws NullPointerException if pools or defaultPoolId is null
     */
    public ResourcePoolManager(List<ResourcePool> pools, String defaultPoolId) {
        Objects.requireNonNull(pools, "pools must not be null");
        this.defaultPoolId = Objects.requireNonNull(defaultPoolId, "defaultPoolId must not be null");
        this.pools = pools.stream()
                .collect(Collectors.toMap(ResourcePool::poolId, Function.identity()));
        pools.forEach(p -> poolStates.put(p.poolId(), new PoolState()));
    }

    /**
     * Resolves the resource pool assigned to a tenant.
     *
     * @param tenant tenant to assign
     * @return assigned pool, or the default pool when necessary
     */
    public ResourcePool assignPool(Tenant tenant) {
        String poolId = tenant.resourcePool() != null ? tenant.resourcePool() : defaultPoolId;
        ResourcePool pool = pools.get(poolId);
        return pool != null ? pool : pools.get(defaultPoolId);
    }

    /**
     * Checks whether a pool can accept another query and its requested memory.
     *
     * @param poolId resource pool identifier
     * @param requestedMemory requested memory in bytes
     * @return true when the pool has sufficient capacity
     */
    public boolean isAvailable(String poolId, long requestedMemory) {
        ResourcePool pool = pools.get(poolId);
        PoolState state = poolStates.get(poolId);
        if (pool == null || state == null) {
            return false;
        }
        return state.runningQueries.get() < pool.maxConcurrentQueries()
                && state.usedMemory.get() + requestedMemory <= pool.memoryBytes();
    }

    /**
     * Records resource acquisition for a query.
     *
     * @param poolId resource pool identifier
     * @param memoryBytes acquired memory in bytes
     */
    public void acquireResources(String poolId, long memoryBytes) {
        PoolState state = poolStates.get(poolId);
        if (state == null) {
            return;
        }
        state.runningQueries.incrementAndGet();
        state.usedMemory.addAndGet(memoryBytes);
    }

    /**
     * Records resource release for a completed query.
     *
     * @param poolId resource pool identifier
     * @param memoryBytes released memory in bytes
     */
    public void releaseResources(String poolId, long memoryBytes) {
        PoolState state = poolStates.get(poolId);
        if (state == null) {
            return;
        }
        state.runningQueries.decrementAndGet();
        state.usedMemory.addAndGet(-memoryBytes);
    }

    private static final class PoolState {
        final AtomicInteger runningQueries = new AtomicInteger(0);
        final AtomicLong usedMemory = new AtomicLong(0);
    }
}
