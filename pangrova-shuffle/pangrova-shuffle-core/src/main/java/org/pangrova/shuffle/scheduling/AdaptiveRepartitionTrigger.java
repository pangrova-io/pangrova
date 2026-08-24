package org.pangrova.shuffle.scheduling;

import java.util.List;
import java.util.Optional;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.shuffle.ShufflePartitionStats;

/**
 * SPI callback invoked after a shuffle stage completes with actual partition
 * statistics. Implementations may return a repartition decision that the
 * scheduler applies before launching downstream tasks.
 *
 * <p>This is the scheduler-side hook for <b>partition-level Adaptive Query
 * Execution (AQE)</b>. By examining real data distribution after a stage
 * finishes, the trigger can:
 * <ul>
 *   <li><b>Coalesce</b> many small partitions into fewer, larger ones
 *       (reduces scheduling overhead and per-task memory waste).</li>
 *   <li><b>Split</b> skewed partitions into sub-ranges to prevent a single
 *       task from becoming a bottleneck.</li>
 *   <li><b>No-op</b> when the distribution is acceptable — return empty.</li>
 * </ul>
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Spark AQE:</b> {@code OptimizeSkewedJoin} + {@code CoalesceShufflePartitions}
 *       rules fire between stages; very similar semantics.</li>
 *   <li><b>Trino:</b> Does not have inter-stage AQE today; this brings that capability
 *       to Pangrova ahead of Trino.</li>
 *   <li><b>Databricks Photon:</b> Uses runtime statistics for partition coalescing
 *       and skew mitigation transparently.</li>
 * </ul>
 *
 * <p>Implementations must be stateless and thread-safe — the scheduler may invoke
 * the trigger concurrently for different stages.</p>
 */
@Experimental(reason = "G-SCHED-07 adaptive repartition trigger for partition-level AQE")
public interface AdaptiveRepartitionTrigger {

    /**
     * Evaluates partition statistics after a stage completes and optionally returns
     * a repartition decision.
     *
     * @param stageId          the completed stage identifier
     * @param partitionStats   per-partition statistics from the completed stage, ordered
     *                         by partition ID
     * @param currentParallelism the current downstream parallelism (number of partitions)
     * @return a repartition decision, or empty if no adjustment is needed
     */
    Optional<RepartitionDecision> evaluate(
            String stageId,
            List<ShufflePartitionStats> partitionStats,
            int currentParallelism);

    /**
     * Describes a repartition adjustment the scheduler should apply before
     * launching downstream tasks.
     *
     * @param newPartitionCount the adjusted partition count for the downstream stage
     * @param reason            human-readable reason for the adjustment (for logging/tracing)
     */
    record RepartitionDecision(int newPartitionCount, String reason) {

        public RepartitionDecision {
            if (newPartitionCount <= 0) {
                throw new IllegalArgumentException(
                        "newPartitionCount must be positive, got: " + newPartitionCount);
            }
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("reason must not be null or blank");
            }
        }

        /**
         * Creates a coalescing decision that reduces partition count.
         *
         * @param newCount the reduced partition count
         * @return coalescing decision
         */
        public static RepartitionDecision coalesce(int newCount) {
            return new RepartitionDecision(newCount,
                    "coalesce small partitions to " + newCount);
        }

        /**
         * Creates a splitting decision that increases partition count for skew mitigation.
         *
         * @param newCount the increased partition count
         * @return splitting decision
         */
        public static RepartitionDecision splitSkew(int newCount) {
            return new RepartitionDecision(newCount,
                    "split skewed partitions to " + newCount);
        }
    }
}
