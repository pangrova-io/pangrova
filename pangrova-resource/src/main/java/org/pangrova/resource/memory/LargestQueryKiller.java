package org.pangrova.resource.memory;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Selects the query with the largest memory consumption for termination,
 * preferring queries from over-quota tenants.
 *
 * <p>Strategy: first try to kill the largest query belonging to an
 * over-quota tenant (ranked by combined Java + Native usage). If no
 * over-quota tenant queries exist, fall back to the globally largest query.</p>
 *
 * <p>When a {@link CrossDomainMemoryCoordinator} is provided, tenant ranking
 * uses combined (Java + Native) usage. Otherwise, only Java-side pool bytes
 * are considered.</p>
 */
@ThreadSafe
public final class LargestQueryKiller implements LowMemoryKiller {

    private final Predicate<String> isOverQuota;
    private final ToLongFunction<String> tenantCombinedUsage;

    /**
     * Creates a killer that uses combined Java + Native usage for tenant ranking.
     *
     * @param isOverQuota          predicate returning true if tenantId exceeds quota
     * @param tenantCombinedUsage  function returning combined bytes for a tenantId
     *                             (typically {@code coordinator::combinedUsage})
     */
    public LargestQueryKiller(Predicate<String> isOverQuota, ToLongFunction<String> tenantCombinedUsage) {
        this.isOverQuota = isOverQuota != null ? isOverQuota : t -> false;
        this.tenantCombinedUsage = tenantCombinedUsage;
    }

    /**
     * Creates a killer with tenant over-quota awareness but no cross-domain usage.
     *
     * @param isOverQuota predicate that returns true if the given tenantId
     *                    is currently exceeding its resource quota.
     *                    Use {@code t -> false} for single-tenant deployments.
     */
    public LargestQueryKiller(Predicate<String> isOverQuota) {
        this(isOverQuota, null);
    }

    /** No tenant awareness, no cross-domain usage. */
    public LargestQueryKiller() {
        this(t -> false, null);
    }

    @Override
    public Optional<QueryMemoryPool> chooseQueryToKill(List<QueryMemoryPool> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Optional.empty();
        }
        // Prefer killing the largest query from an over-quota tenant.
        // When cross-domain coordinator is available, rank by combined usage.
        Optional<QueryMemoryPool> overQuotaVictim = candidates.stream()
                .filter(q -> isOverQuota.test(q.getTenantId()))
                .max(Comparator.comparingLong(this::effectiveUsage));
        if (overQuotaVictim.isPresent()) {
            return overQuotaVictim;
        }
        // Fallback: globally largest query by effective usage
        return candidates.stream()
                .max(Comparator.comparingLong(this::effectiveUsage));
    }

    /**
     * Returns the effective usage for ranking: combined (Java + Native) if
     * cross-domain coordinator is available, otherwise Java pool bytes only.
     */
    private long effectiveUsage(QueryMemoryPool pool) {
        if (tenantCombinedUsage != null) {
            return tenantCombinedUsage.applyAsLong(pool.getTenantId());
        }
        return pool.getUsedBytes();
    }
}
