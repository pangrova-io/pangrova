package org.pangrova.operations.ddl;

import org.pangrova.operations.enums.DdlJobState;
import org.pangrova.operations.enums.DdlJobType;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents an asynchronous DDL operation with lifecycle tracking.
 */
public record DdlJob(
        String jobId,
        DdlJobType type,
        String ddlStatement,
        DdlJobState state,
        double progress,
        Instant createdAt,
        Instant updatedAt,
        String errorMessage) {

    public DdlJob {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(ddlStatement, "ddlStatement");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public DdlJob withState(DdlJobState newState) {
        return new DdlJob(jobId, type, ddlStatement, newState, progress, createdAt, Instant.now(), errorMessage);
    }

    public DdlJob withError(String error) {
        return new DdlJob(jobId, type, ddlStatement, DdlJobState.FAILED, progress, createdAt, Instant.now(), error);
    }
}
