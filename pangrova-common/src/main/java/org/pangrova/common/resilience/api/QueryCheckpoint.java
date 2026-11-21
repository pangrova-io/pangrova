package org.pangrova.common.resilience.api;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * SPI for query-level checkpoint and resume during fault-tolerant execution.
 *
 * <p>Enables stage-boundary checkpoint/resume: when a stage completes, its output
 * metadata (partition counts, row counts, shuffle handles) can be checkpointed so
 * that after a coordinator or worker failure, execution resumes from the last
 * completed stage instead of restarting the entire query.</p>
 *
 * <h3>Recovery floor</h3>
 * <p>The recovery floor for a query is determined by the combination of:</p>
 * <ul>
 *   <li>This checkpoint SPI (stage-boundary metadata)</li>
 *   <li>Verified reachability and selected-attempt publication of Shuffle data</li>
 *   <li>The FTE stage scheduler's retry budget</li>
 * </ul>
 *
 * <p>Without durable shuffle, checkpoints alone are insufficient: the stage output
 * data would be lost even though the metadata says the stage completed. The FTE
 * recovery owner must verify the current exchange receipt, selected attempts and effective
 * consumer-retry guarantees before checkpoint-based resume. This SPI supplies no such proof.</p>
 *
 * <h3>Checkpoint granularity</h3>
 * <p>The current granularity is <b>stage boundary</b> (coarse): all tasks in a stage
 * must complete before a checkpoint is taken. Finer-grained operator-level checkpoints
 * (e.g., HashBuild/Sort partial state to NVMe) are a future extension tracked as
 * OPT-023 and are not part of this SPI.</p>
 *
 * <p>Implementations must be thread-safe. Concurrent calls from different threads
 * (e.g., multiple stage completions in parallel) must be serialized by the backend.</p>
 *
 * @see CheckpointStore
 */
@Experimental(reason = "G-HIGH_AVAILABILITY-04: stage-boundary checkpoint/resume for fault-tolerant-execution; "
        + "depends on durable shuffle (G-HIGH_AVAILABILITY-07) being available")
public interface QueryCheckpoint {

    /**
     * Immutable snapshot of a completed stage's output metadata.
     *
     * @param queryId   the owning query identifier
     * @param stageId   the completed stage identifier
     * @param outputPartitions number of output partitions produced
     * @param outputRows      total rows produced across all partitions
     * @param shuffleHandles  opaque shuffle handle references for each partition
     *                        (encoded as strings for serialization)
     * @param checkpointedAtMs timestamp when the checkpoint was taken
     */
    record StageCheckpoint(
            String queryId,
            String stageId,
            int outputPartitions,
            long outputRows,
            List<String> shuffleHandles,
            long checkpointedAtMs) {

        public StageCheckpoint {
            if (queryId == null || queryId.isBlank()) {
                throw new IllegalArgumentException("queryId must not be blank");
            }
            if (stageId == null || stageId.isBlank()) {
                throw new IllegalArgumentException("stageId must not be blank");
            }
            if (outputPartitions < 0) {
                throw new IllegalArgumentException("outputPartitions must be >= 0");
            }
            if (outputRows < 0) {
                throw new IllegalArgumentException("outputRows must be >= 0");
            }
            shuffleHandles = shuffleHandles != null
                    ? List.copyOf(shuffleHandles) : List.of();
        }
    }

    /**
     * Records a stage-boundary checkpoint after all tasks in the stage complete.
     *
     * <p>Must be called after the stage's shuffle data is committed (i.e., after
     * all partition writers have finalized). The checkpoint is durable only if the
     * underlying store is persistent (JDBC/NVMe).</p>
     *
     * @param checkpoint the stage completion metadata
     */
    void checkpointStage(StageCheckpoint checkpoint);

    /**
     * Returns the latest checkpoint for a given stage of a query, if one exists.
     *
     * @param queryId the query identifier
     * @param stageId the stage identifier
     * @return the checkpoint metadata, or empty if no checkpoint exists
     */
    Optional<StageCheckpoint> getStageCheckpoint(String queryId, String stageId);

    /**
     * Returns all checkpointed stages for a query, ordered by checkpoint time.
     *
     * <p>Used during recovery to determine the recovery floor: the last stage
     * whose checkpoint is valid (shuffle data still available).</p>
     *
     * @param queryId the query identifier
     * @return list of stage checkpoints, ordered by {@code checkpointedAtMs} ascending
     */
    List<StageCheckpoint> getQueryCheckpoints(String queryId);

    /**
     * Removes all checkpoints for a completed query (periodic cleanup).
     *
     * @param queryId the query identifier
     */
    void clearQueryCheckpoints(String queryId);

    /**
     * Checks whether the given stage has a valid checkpoint that can be used
     * for resume (i.e., checkpoint exists and shuffle data is still available).
     *
     * <p>The default implementation checks only for checkpoint existence.
     * Implementations backed by durable shuffle should additionally verify
     * that the shuffle handles are still valid.</p>
     *
     * @param queryId the query identifier
     * @param stageId the stage identifier
     * @return true if the stage can be resumed from its checkpoint
     */
    default boolean canResumeStage(String queryId, String stageId) {
        return getStageCheckpoint(queryId, stageId).isPresent();
    }
}
