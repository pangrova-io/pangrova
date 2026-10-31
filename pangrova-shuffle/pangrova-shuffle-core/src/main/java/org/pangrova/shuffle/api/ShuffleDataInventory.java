package org.pangrova.shuffle.api;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Tracks committed shuffle data across workers for scale-down and preemption decisions (G-RES-08).
 *
 * <p>The coordinator's scheduler consults this inventory before decommissioning a worker:
 * <ol>
 *   <li>Query {@link #getLiveDataOnWorker(String)} to find unreferenced shuffle data.</li>
 *   <li>If any entries have {@link ShuffleRetentionPolicy#EPHEMERAL} retention, the scheduler
 *       must either migrate the data (if the provider supports it), mark upstream stages for
 *       re-execution, or block scale-down.</li>
 *   <li>For {@link ShuffleRetentionPolicy#DETACHED} data, the scheduler allows scale-down
 *       after the worker's drain window completes.</li>
 *   <li>For {@link ShuffleRetentionPolicy#PERSISTENT} data, scale-down proceeds immediately.</li>
 * </ol>
 *
 * <p>Implementations may be backed by the coordinator's stage tracker, the shuffle lifecycle
 * manager, or an external metadata store. The default in-tree implementation tracks data
 * references in memory, populated by stage commit callbacks.
 */
@Experimental(reason = "G-RES-08: shuffle data inventory for safe scale-down")
public interface ShuffleDataInventory {

    /**
     * Returns all live (unconsumed) shuffle data references held by a specific worker.
     *
     * @param workerId the worker to query
     * @return live shuffle data refs on the worker, or an empty list if none
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
     * Checks whether a worker can be safely decommissioned given the current shuffle
     * capabilities and the data it holds.
     *
     * <p>A worker is safe to decommission when either:
     * <ul>
     *   <li>It holds no live shuffle data, or</li>
     *   <li>All live shuffle data has {@link ShuffleRetentionPolicy#PERSISTENT} retention, or</li>
     *   <li>All live shuffle data has at least {@link ShuffleRetentionPolicy#DETACHED} retention
     *       (safe with a drain window).</li>
     * </ul>
     *
     * @param workerId the worker to check
     * @return the decommission readiness assessment
     */
    default DecommissionAssessment assessDecommission(String workerId) {
        List<ShuffleDataRef> liveData = getLiveDataOnWorker(workerId);
        if (liveData.isEmpty()) {
            return DecommissionAssessment.SAFE;
        }
        boolean hasEphemeral = liveData.stream().anyMatch(ShuffleDataRef::isEphemeral);
        if (hasEphemeral) {
            long ephemeralBytes = liveData.stream()
                    .filter(ShuffleDataRef::isEphemeral)
                    .mapToLong(ShuffleDataRef::sizeBytes)
                    .sum();
            return new DecommissionAssessment(
                    DecommissionAssessment.Readiness.REQUIRES_RE_EXECUTION,
                    liveData.stream().filter(ShuffleDataRef::isEphemeral).toList(),
                    ephemeralBytes);
        }
        boolean allPersistent = liveData.stream()
                .allMatch(ref -> ref.retention() == ShuffleRetentionPolicy.PERSISTENT);
        if (allPersistent) {
            return DecommissionAssessment.SAFE;
        }
        // Some DETACHED data — safe with drain
        return new DecommissionAssessment(
                DecommissionAssessment.Readiness.SAFE_WITH_DRAIN,
                List.of(),
                0);
    }
}
