package org.pangrova.shuffle.api;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Reported shuffle references and an implementation-owned decommission assessment.
 * A list is not a completeness proof. Assessment requires authoritative inventory coverage,
 * worker incarnation and admission fencing, selected exchange generations/attempts, active
 * reader ownership and the effective provider guarantee. Retention or an empty list alone
 * cannot establish those facts. Missing evidence must be reported as UNKNOWN.
 */
@Experimental(reason = "G-RES-08: shuffle data inventory for safe scale-down")
public interface ShuffleDataInventory {

    /**
     * Returns all live (unconsumed) shuffle data references held by a specific worker.
     *
     * @param workerId the worker to query
     * @return reported live references; an empty list is not proof that the worker holds no data
     */
    List<ShuffleDataRef> getLiveDataOnWorker(String workerId);

    /**
     * Records that a shuffle stage has committed data on a worker.
     *
     * @param ref the shuffle data reference to register
     */
    void register(ShuffleDataRef ref);

    /**
     * Marks a shuffle data reference as consumed (all downstream readers have finished).
     *
     * @param queryId  the owning query
     * @param stageId  the producing stage
     * @param workerId the worker holding the data
     */
    void markConsumed(String queryId, int stageId, String workerId);

    /**
     * Removes all shuffle data references for a completed or failed query.
     *
     * @param queryId the query to clean up
     */
    void removeQuery(String queryId);

    /**
     * Assesses removal using the implementation's current authoritative evidence.
     * The result is advisory, not a transferable removal permit: the removal owner must keep
     * admission closed and the same worker incarnation/evidence valid through removal.
     * Implementations without a complete, fenced view must return UNKNOWN, including for an
     * empty or consumed-only list. Persistent/remote storage does not imply survival guarantees.
     */
    DecommissionAssessment assessDecommission(String workerId);
}
