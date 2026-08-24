package org.pangrova.operations.ddl;

import java.util.List;
import java.util.Optional;

/**
 * Persistence abstraction for DDL job state.
 */
public interface DdlJobStore {

    void save(DdlJob job);

    Optional<DdlJob> findById(String jobId);

    List<DdlJob> findAll();

    void updateState(String jobId, DdlJob.DdlJobState state);

    /**
     * Atomically updates job state only if current state matches {@code expectedState}.
     *
     * <p><strong>WARNING:</strong> The default implementation is NOT atomic — it performs
     * a read-then-write sequence with a TOCTOU gap. Production implementations backed by
     * a database MUST override this method with a single atomic operation, e.g.,
     * {@code UPDATE ddl_jobs SET state = ? WHERE job_id = ? AND state = ?}.</p>
     *
     * <p>The default is provided only for in-memory or single-threaded test usage.</p>
     *
     * @return true if the update was applied, false if the current state did not match
     */
    default boolean compareAndUpdateState(String jobId, DdlJob.DdlJobState expectedState,
                                           DdlJob.DdlJobState newState) {
        Optional<DdlJob> job = findById(jobId);
        if (job.isPresent() && job.get().state() == expectedState) {
            updateState(jobId, newState);
            return true;
        }
        return false;
    }
}
