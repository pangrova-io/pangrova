package org.pangrova.coordinator.dynamictable;

import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.RefreshHistoryEntry;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Detailed info for DESCRIBE DYNAMIC TABLE output.
 *
 * <p>Includes recent refresh history entries (R23 AC4).</p>
 */
public record DynamicTableDetail(
        String dtName,
        String tenantId,
        DynamicTableLifecycleState state,
        DynamicTableRefreshMode refreshMode,
        Duration targetLag,
        String checkpoint,
        String checkpointType,
        int sourceSchemaVersion,
        String creatorIdentity,
        Instant lastRefreshTime,
        long totalRefreshCount,
        int consecutiveFailures,
        Instant createdAt,
        List<RefreshHistoryEntry> recentRefreshes
) {

    public DynamicTableDetail {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(refreshMode, "refreshMode must not be null");
        Objects.requireNonNull(targetLag, "targetLag must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(recentRefreshes, "recentRefreshes must not be null");
        recentRefreshes = List.copyOf(recentRefreshes);
    }
}
