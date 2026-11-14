package org.pangrova.coordinator.clustering;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory tracker for {@code RECLUSTER TABLE} jobs keyed by task_id.
 *
 * <p>Returned task ids are real handles: {@link #find(String)} returns the
 * current state, error message, and timing data so {@code SHOW CLUSTERING
 * STATUS} (or a future {@code SHOW CLUSTERING JOB}) can surface progress.</p>
 *
 * <p>Persistent backends (e.g., a database-backed implementation) can replace
 * this class via the same package — the tracker is referenced by interface
 * shape, not name, by {@link ClusteringScheduler}.</p>
 */
@ThreadSafe
public final class ReclusterJobTracker {

    private final ConcurrentMap<String, ReclusterJob> jobs = new ConcurrentHashMap<>();

    /** Records a newly submitted job. */
    public void register(ReclusterJob job) {
        jobs.put(job.taskId(), job);
    }

    /**
     * Atomically transitions a tracked job's state. No-op if the task is unknown.
     */
    public void transition(String taskId, ReclusterJob.State newState) {
        jobs.computeIfPresent(taskId, (id, existing) -> existing.withState(newState));
    }

    /**
     * Marks a tracked job as FAILED with the given error message.
     * No-op if the task is unknown.
     */
    public void fail(String taskId, String errorMessage) {
        jobs.computeIfPresent(taskId, (id, existing) -> existing.withFailure(errorMessage));
    }

    public Optional<ReclusterJob> find(String taskId) {
        return Optional.ofNullable(jobs.get(taskId));
    }

    public List<ReclusterJob> listAll() {
        return List.copyOf(jobs.values());
    }
}
