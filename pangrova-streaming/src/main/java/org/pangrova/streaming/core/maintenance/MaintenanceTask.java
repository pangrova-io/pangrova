package org.pangrova.streaming.core.maintenance;

import java.time.Instant;

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
