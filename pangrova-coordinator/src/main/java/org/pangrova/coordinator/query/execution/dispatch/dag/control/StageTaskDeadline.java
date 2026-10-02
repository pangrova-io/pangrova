package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.spi.annotation.Internal;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Explicit immutable execution deadline, bounded by an actual durable lease when applicable. */
@Internal
public record StageTaskDeadline(long epochMillis, Instant preparedAt) {
    /** Requires a positive, non-expired deadline at the actual preparation clock. */
    public StageTaskDeadline {
        Objects.requireNonNull(preparedAt, "preparedAt");
        if (epochMillis <= preparedAt.toEpochMilli()) {
            throw new IllegalStateException("Task execution lease expired before dispatch");
        }
    }

    /** Live tasks receive an explicit bounded duration, without an implicit default. */
    public static StageTaskDeadline after(Duration duration, Instant now) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("Task execution duration must be positive");
        }
        return new StageTaskDeadline(Math.addExact(now.toEpochMilli(), duration.toMillis()), now);
    }

    /** Database renewal cannot extend a previously dispatched partition's Native credentials. */
    public static StageTaskDeadline forPartition(
            ArtifactBuildLease lease, ArtifactBuildDispatchSpec spec, Instant now) {
        return new StageTaskDeadline(Math.min(lease.expiresAt().toEpochMilli(),
                Math.addExact(now.toEpochMilli(), spec.taskTimeoutMillis())), now);
    }
}
