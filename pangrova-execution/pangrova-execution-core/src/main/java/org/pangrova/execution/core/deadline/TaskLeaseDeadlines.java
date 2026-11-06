package org.pangrova.execution.core.deadline;

import org.pangrova.execution.api.deadline.StageTaskDeadline;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.page.GenerationLease;
import java.time.Instant;

/** Binds a task deadline to the actual claimed generation lease. */
public final class TaskLeaseDeadlines {
    private TaskLeaseDeadlines() {}

    /** Database renewal cannot extend a previously dispatched partition's Native credentials. */
    public static StageTaskDeadline forPartition(
            GenerationLease lease, GenerationDispatchSpec spec, Instant now) {
        return new StageTaskDeadline(Math.min(lease.expiresAt().toEpochMilli(),
                Math.addExact(now.toEpochMilli(), spec.taskTimeoutMillis())), now);
    }
}
