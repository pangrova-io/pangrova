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
 * <p>Translates registered pool definitions to {@link CpuSchedulingConfig} entries.
 * This is an experimental configuration component: no production assembly currently
 * registers this provider or consumes it in Worker/Native CPU scheduling. Registering
 * a pool here does not enforce CPU shares, dedicated cores or execution concurrency.</p>
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
