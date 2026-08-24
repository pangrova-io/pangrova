package org.pangrova.security.tenant.pool;

import org.pangrova.security.tenant.model.ResourcePool;
import org.pangrova.spi.resource.scheduling.CpuSchedulingConfig;
import org.pangrova.spi.resource.scheduling.CpuSchedulingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link CpuSchedulingProvider} backed by {@link ResourcePool} definitions.
 *
 * <p>Bridges the gap between tenant resource pool configuration (which already
 * carries {@code cpuShare}) and the CPU scheduling SPI that the coordinator's
 * {@code TaskScheduler} and worker's {@code WeightedFairScheduler} consume.</p>
 *
 * <p>Resolves G-RES-02: the existing {@code ResourcePool.cpuShare()} field was
 * defined but never fed to any scheduling component. This provider reads pool
 * definitions and translates them to {@link CpuSchedulingConfig} entries.</p>
 *
 * <p>On startup, the coordinator populates this provider from
 * {@link ResourcePoolManager}'s pools; dynamic updates (ALTER RESOURCE GROUP)
 * are applied via {@link #updateConfig}.</p>
 */
public final class ResourcePoolCpuSchedulingProvider implements CpuSchedulingProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ResourcePoolCpuSchedulingProvider.class);

    private final ConcurrentHashMap<String, CpuSchedulingConfig> configs = new ConcurrentHashMap<>();

    /**
     * Registers a resource pool's CPU scheduling configuration.
     *
     * @param pool the resource pool definition
     */
    public void registerPool(ResourcePool pool) {
        int weight = Math.max(1, (int) (pool.cpuShare() * CpuSchedulingConfig.DEFAULT_CPU_WEIGHT));
        CpuSchedulingConfig config = CpuSchedulingConfig.weighted(
                pool.poolId(), weight, pool.maxConcurrentQueries());
        configs.put(pool.poolId(), config);
        LOG.info("event=cpu_scheduling_registered poolId={} weight={} maxConcurrency={}",
                pool.poolId(), weight, pool.maxConcurrentQueries());
    }

    @Override
    public Optional<CpuSchedulingConfig> getConfig(String groupId) {
        return Optional.ofNullable(configs.get(groupId));
    }

    @Override
    public void updateConfig(CpuSchedulingConfig config) {
        configs.put(config.groupId(), config);
        LOG.info("event=cpu_scheduling_updated groupId={} exclusiveCores={} weight={} maxConcurrency={}",
                config.groupId(), config.exclusiveCpuCores(), config.cpuWeight(), config.maxConcurrency());
    }
}
