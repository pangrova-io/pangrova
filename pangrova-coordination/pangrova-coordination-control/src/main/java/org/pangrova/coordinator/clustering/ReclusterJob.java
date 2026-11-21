package org.pangrova.coordinator.clustering;

import org.pangrova.coordinator.enums.ReclusterJobState;

import java.time.Instant;
import java.util.Objects;

/**
 * Snapshot of a RECLUSTER job's lifecycle state.
 *
 * <p>Tracked by {@link ReclusterJobTracker} so users can poll progress
 * by the {@code task_id} returned from {@code RECLUSTER TABLE}.</p>
 *
 * @param taskId       UUID returned to the SQL caller; usable for status queries
 * @param tableName    fully-qualified table being reclustered
 * @param state        current lifecycle state
 * @param submittedAt  when the job was submitted
 * @param updatedAt    last state transition time
 * @param errorMessage failure detail when {@link State#FAILED}, otherwise {@code null}
 */
public record ReclusterJob(
        String taskId,
        String tableName,
        ReclusterJobState state,
        Instant submittedAt,
        Instant updatedAt,
        String errorMessage) {

    public ReclusterJob {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(submittedAt, "submittedAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public ReclusterJob withState(ReclusterJobState newState) {
        return new ReclusterJob(taskId, tableName, newState, submittedAt, Instant.now(), errorMessage);
    }

    public ReclusterJob withFailure(String message) {
        return new ReclusterJob(taskId, tableName, ReclusterJobState.FAILED, submittedAt, Instant.now(), message);
    }
}
