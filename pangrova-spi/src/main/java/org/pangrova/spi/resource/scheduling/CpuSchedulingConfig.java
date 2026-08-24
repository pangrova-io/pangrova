package org.pangrova.spi.resource.scheduling;

import org.pangrova.spi.annotation.Experimental;

/**
 * Per-tenant or per-resource-group CPU scheduling configuration.
 *
 * <p>Models the StarRocks-style exclusive_cpu_cores / cpu_weight isolation:
 * <ul>
 *   <li><b>Exclusive mode</b> ({@code exclusiveCpuCores > 0}): the group gets
 *       dedicated threads pinned to the specified number of cores. Other groups
 *       cannot borrow from this allocation.</li>
 *   <li><b>Weighted mode</b> ({@code exclusiveCpuCores == 0, cpuWeight > 0}):
 *       the group shares a common thread pool but gets a proportional share based
 *       on weight. Under contention, CPU time is divided according to weight
 *       ratios; under no contention, any group may use all available cores.</li>
 * </ul>
 *
 * <p>This is a pure data model; the executor-side enforcement is in
 * {@code WeightedFairScheduler} (weighted mode) or a future cgroup/YARN binding
 * (exclusive mode).</p>
 *
 * <p>Resolves G-RES-02: adds a declarative CPU isolation SPI that the
 * coordinator admission gate and worker task scheduler can consume.</p>
 *
 * @param groupId           resource group or tenant identifier
 * @param exclusiveCpuCores number of dedicated CPU cores (0 = weighted mode)
 * @param cpuWeight         relative weight for fair-share scheduling (0..100, default 10)
 * @param maxConcurrency    max concurrent tasks for this group (0 = unlimited)
 */
@Experimental(reason = "G-RES-02: CPU isolation SPI — per-group/tenant CPU scheduling")
public record CpuSchedulingConfig(
        String groupId,
        int exclusiveCpuCores,
        int cpuWeight,
        int maxConcurrency) {

    /** Default weight — groups with no explicit weight get this share. */
    public static final int DEFAULT_CPU_WEIGHT = 10;

    public CpuSchedulingConfig {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be null or blank");
        }
        if (exclusiveCpuCores < 0) {
            throw new IllegalArgumentException("exclusiveCpuCores must be >= 0, got " + exclusiveCpuCores);
        }
        if (cpuWeight < 0) {
            throw new IllegalArgumentException("cpuWeight must be >= 0, got " + cpuWeight);
        }
        if (maxConcurrency < 0) {
            throw new IllegalArgumentException("maxConcurrency must be >= 0, got " + maxConcurrency);
        }
    }

    /** Creates a weighted-mode config (no exclusive cores). */
    public static CpuSchedulingConfig weighted(String groupId, int cpuWeight, int maxConcurrency) {
        return new CpuSchedulingConfig(groupId, 0, cpuWeight, maxConcurrency);
    }

    /** Creates an exclusive-mode config with dedicated cores. */
    public static CpuSchedulingConfig exclusive(String groupId, int cores, int maxConcurrency) {
        return new CpuSchedulingConfig(groupId, cores, 0, maxConcurrency);
    }

    /** Creates a default config (weighted mode, default weight, unlimited concurrency). */
    public static CpuSchedulingConfig defaultConfig(String groupId) {
        return new CpuSchedulingConfig(groupId, 0, DEFAULT_CPU_WEIGHT, 0);
    }

    /** @return true if this group uses exclusive CPU cores. */
    public boolean isExclusive() {
        return exclusiveCpuCores > 0;
    }
}
