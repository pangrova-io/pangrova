package org.pangrova.operations.ddl;

import java.util.List;
import java.util.Optional;

/**
 * Persistence abstraction for DDL job state.
 */
public interface DdlJobStore {

    /** Inserts a new job and rejects duplicate job IDs without changing the existing job. */
    void insert(DdlJob job);

    Optional<DdlJob> findById(String jobId);

    List<DdlJob> findAll();

    /** Atomically replaces the expected state. Missing jobs and competing transitions return false. */
    boolean compareAndUpdateState(String jobId, DdlJob.DdlJobState expectedState, DdlJob.DdlJobState newState);
}
