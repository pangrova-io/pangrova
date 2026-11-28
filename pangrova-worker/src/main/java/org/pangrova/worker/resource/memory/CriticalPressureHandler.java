package org.pangrova.worker.resource.memory;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Handles CRITICAL memory pressure by selecting and terminating the largest
 * query on the node. Invoked by {@code ResourceManager} when spill alone
 * cannot reclaim sufficient memory.
 *
 * <p>Concurrency: stateless — all mutable state lives in the injected
 * collaborators. Safe to call from any thread.</p>
 */
@ThreadSafe
public final class CriticalPressureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CriticalPressureHandler.class);

    private final LowMemoryKiller killer;
    private final Supplier<List<QueryMemoryPool>> activeQueryPoolsSupplier;
    private final Consumer<QueryMemoryPool> queryKillAction;

    /**
     * @param killer                   strategy for selecting the victim query
     * @param activeQueryPoolsSupplier supplies active query pools on this node
     * @param queryKillAction          callback to terminate the selected query
     */
    public CriticalPressureHandler(LowMemoryKiller killer,
                                   Supplier<List<QueryMemoryPool>> activeQueryPoolsSupplier,
                                   Consumer<QueryMemoryPool> queryKillAction) {
        if (killer == null) {
            throw new IllegalArgumentException("killer must not be null");
        }
        if (activeQueryPoolsSupplier == null) {
            throw new IllegalArgumentException("activeQueryPoolsSupplier must not be null");
        }
        if (queryKillAction == null) {
            throw new IllegalArgumentException("queryKillAction must not be null");
        }
        this.killer = killer;
        this.activeQueryPoolsSupplier = activeQueryPoolsSupplier;
        this.queryKillAction = queryKillAction;
    }

    /**
     * Selects and terminates the largest query to relieve CRITICAL pressure.
     *
     * @return true if a query was selected and kill was triggered, false otherwise
     */
    public boolean killLargestQuery() {
        List<QueryMemoryPool> candidates = activeQueryPoolsSupplier.get();
        Optional<QueryMemoryPool> victim = killer.chooseQueryToKill(candidates);

        if (victim.isEmpty()) {
            LOG.warn("event=critical_pressure_no_victim candidateCount={}",
                    candidates != null ? candidates.size() : 0);
            return false;
        }

        QueryMemoryPool pool = victim.get();
        LOG.warn("event=critical_pressure_kill queryId={} tenantId={} usedBytes={} reason=memory_pressure_critical",
                pool.getName(), pool.getTenantId(), pool.getUsedBytes());
        queryKillAction.accept(pool);
        return true;
    }
}
