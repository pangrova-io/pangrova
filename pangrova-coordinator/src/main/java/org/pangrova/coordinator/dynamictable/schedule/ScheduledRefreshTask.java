package org.pangrova.coordinator.dynamictable.schedule;

import org.pangrova.metadata.dynamictable.TriggerType;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable descriptor for a scheduled Dynamic Table refresh task.
 *
 * @param dtName        Dynamic Table name
 * @param tenantId      owning tenant identifier
 * @param scheduledTime when this refresh was scheduled to fire
 * @param triggerType   how this refresh was initiated
 */
public record ScheduledRefreshTask(
        String dtName,
        String tenantId,
        Instant scheduledTime,
        TriggerType triggerType
) {

    public ScheduledRefreshTask {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(scheduledTime, "scheduledTime must not be null");
        Objects.requireNonNull(triggerType, "triggerType must not be null");
    }
}
