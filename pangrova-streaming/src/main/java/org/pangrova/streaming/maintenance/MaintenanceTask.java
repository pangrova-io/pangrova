package org.pangrova.streaming.maintenance;

import java.time.Instant;

import org.pangrova.streaming.maintenance.policy.TriggerPriority;

/**
 * A maintenance task with priority ordering.
 */
public record MaintenanceTask(
    String tableName,
    TriggerPriority priority,
    Instant createdAt
) implements Comparable<MaintenanceTask> {

    @Override
    public int compareTo(MaintenanceTask other) {
        return this.priority.compareTo(other.priority);
    }
}
