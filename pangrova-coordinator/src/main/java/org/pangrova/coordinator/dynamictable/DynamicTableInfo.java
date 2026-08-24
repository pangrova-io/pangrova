package org.pangrova.coordinator.dynamictable;

import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Summary info for SHOW DYNAMIC TABLES output.
 *
 * @param dtName          Dynamic Table name
 * @param tenantId        owning tenant
 * @param state           current lifecycle state
 * @param refreshMode     effective refresh mode
 * @param targetLag       declared data freshness SLA
 * @param lastRefreshTime timestamp of last successful refresh (nullable)
 * @param totalRefreshCount cumulative successful refresh count
 */
public record DynamicTableInfo(
        String dtName,
        String tenantId,
        DynamicTableLifecycleState state,
        DynamicTableRefreshMode refreshMode,
        Duration targetLag,
        Instant lastRefreshTime,
        long totalRefreshCount
) {

    public DynamicTableInfo {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(refreshMode, "refreshMode must not be null");
        Objects.requireNonNull(targetLag, "targetLag must not be null");
    }
}
