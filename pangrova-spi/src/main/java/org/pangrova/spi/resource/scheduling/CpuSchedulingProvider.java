package org.pangrova.spi.resource.scheduling;

import org.pangrova.spi.annotation.Experimental;

import java.util.Optional;

/**
 * SPI for resolving per-tenant/per-group CPU scheduling configuration.
 *
 * <p>Implementations load configuration from the tenant store, config files,
 * or an external resource manager and expose it to the coordinator's
 * {@code TaskScheduler} and worker's {@code WeightedFairScheduler}.</p>
 *
 * <p>Resolves G-RES-02: provides the missing link between tenant resource
 * definitions (which already carry {@code cpuShare} in {@code ResourcePool})
 * and the actual scheduling layer.</p>
 *
 * <p>Thread-safety: implementations must be safe for concurrent access from
 * the scheduler hot path.</p>
 */
@Experimental(reason = "G-RES-02: CPU scheduling SPI — resolves per-tenant CPU config")
public interface CpuSchedulingProvider {

    /**
     * Resolves the CPU scheduling configuration for the given resource group
     * or tenant identifier.
     *
     * @param groupId resource group or tenant identifier
     * @return the scheduling config, or empty if no specific config exists
     *         (the caller should use {@link CpuSchedulingConfig#defaultConfig})
     */
    Optional<CpuSchedulingConfig> getConfig(String groupId);

    /**
     * Called when a tenant's or group's CPU configuration changes (e.g. via
     * ALTER RESOURCE GROUP). Implementations should update their internal
     * state so subsequent {@link #getConfig} calls reflect the change.
     *
     * @param config the new configuration
     */
    void updateConfig(CpuSchedulingConfig config);
}
