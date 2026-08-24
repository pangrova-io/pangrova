package org.pangrova.security.tenant.pool;

import org.pangrova.security.tenant.model.ResourcePool;
import org.pangrova.security.tenant.model.SchedulingPolicy;
import org.pangrova.spi.resource.scheduling.CpuSchedulingConfig;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates G-RES-02: ResourcePoolCpuSchedulingProvider bridges ResourcePool
 * definitions to the CpuSchedulingConfig SPI.
 */
class ResourcePoolCpuSchedulingProviderTest {

    @Test
    void registeredPoolIsResolvable() {
        ResourcePoolCpuSchedulingProvider provider = new ResourcePoolCpuSchedulingProvider();
        ResourcePool pool = new ResourcePool(
                "interactive-pool", "Interactive",
                2.0,             // cpuShare = 2x
                8L * 1024 * 1024 * 1024, // 8 GB
                20,              // maxConcurrentQueries
                50,              // queueCapacity
                SchedulingPolicy.FAIR,
                10, false,
                0.1, 1.0, true);

        provider.registerPool(pool);

        Optional<CpuSchedulingConfig> config = provider.getConfig("interactive-pool");
        assertThat(config).isPresent();
        assertThat(config.get().cpuWeight()).isEqualTo(20); // 2.0 * DEFAULT_WEIGHT(10)
        assertThat(config.get().maxConcurrency()).isEqualTo(20);
        assertThat(config.get().isExclusive()).isFalse();
    }

    @Test
    void unregisteredPoolReturnsEmpty() {
        ResourcePoolCpuSchedulingProvider provider = new ResourcePoolCpuSchedulingProvider();
        assertThat(provider.getConfig("unknown")).isEmpty();
    }

    @Test
    void updateConfigOverridesRegisteredPool() {
        ResourcePoolCpuSchedulingProvider provider = new ResourcePoolCpuSchedulingProvider();
        ResourcePool pool = new ResourcePool(
                "batch-pool", "Batch",
                1.0, 4L * 1024 * 1024 * 1024, 10, 100,
                SchedulingPolicy.FIFO, 5, true, 0.1, 1.0, false);
        provider.registerPool(pool);

        // Override with exclusive config
        CpuSchedulingConfig exclusive = CpuSchedulingConfig.exclusive("batch-pool", 4, 8);
        provider.updateConfig(exclusive);

        Optional<CpuSchedulingConfig> config = provider.getConfig("batch-pool");
        assertThat(config).isPresent();
        assertThat(config.get().isExclusive()).isTrue();
        assertThat(config.get().exclusiveCpuCores()).isEqualTo(4);
    }
}
