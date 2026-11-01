package org.pangrova.shuffle.api.adaptive;

import org.pangrova.spi.annotation.Experimental;

/**
 * Strategy for server-side partition merge in the Remote Shuffle Service (ADR-002 D2).
 *
 * <p>Without server-side merge, each reducer must issue M separate small reads
 * (one per upstream map task) for each partition, resulting in O(M x N) total
 * read operations across N partitions. This creates excessive small I/O and
 * network round-trips that degrade shuffle throughput.</p>
 *
 * <p>A {@code PartitionMergeStrategy} controls how the shuffle service combines
 * per-mapper partition fragments into a single sequential stream per partition
 * before serving to reducers. This reduces read operations from O(M x N) to O(N)
 * and enables large sequential reads instead of many small random reads.</p>
 *
 * <h3>Strategies</h3>
 * <ul>
 *   <li>{@link #NONE} — No merge; each mapper's contribution is served separately.
 *       This is the legacy behavior where reducers do M x N small reads.</li>
 *   <li>{@link #APPEND} — Committed batches from all mappers for the same partition
 *       are concatenated into a single sequential stream (Magnet-style sort-merge
 *       or Celeborn-style append). Reducers issue one large sequential read per
 *       partition. This is the recommended default for push-style shuffle.</li>
 *   <li>{@link #SORT_MERGE} — Batches are merge-sorted by a partition key before
 *       serving. Produces a globally-sorted sequential stream per partition,
 *       eliminating the need for reducer-side sort. Higher server-side cost but
 *       eliminates reducer sort entirely.</li>
 * </ul>
 *
 * <p>The strategy is configured per shuffle registration and may vary by provider:
 * Celeborn uses APPEND (sort at reducer), object-store uses APPEND (concatenated
 * objects), and a future sort-shuffle service could use SORT_MERGE.</p>
 *
 * <p>Industry references: LinkedIn Magnet (sort-merge), Celeborn (append with
 * optional sort), Spark push-based shuffle (server-side merge).</p>
 *
 * @see ShuffleLifecycleManager#registerShuffle(ShuffleRegistration)
 */
@Experimental(reason = "ADR-002 D2: server-side partition merge to eliminate M x N small reads")
public enum PartitionMergeStrategy {

    /**
     * No server-side merge. Each mapper's partition contribution is served as a
     * separate stream. Reducers must issue one read per mapper per partition (M x N).
     * This is the legacy pull-from-producer behavior.
     */
    NONE,

    /**
     * Append-merge: all committed batches from different mappers for the same
     * partition are concatenated into a single sequential byte stream. Reducers
     * issue one large sequential read per partition. Data ordering within the
     * merged stream reflects mapper completion order (not sorted).
     *
     * <p>This is the default for Celeborn and object-store providers. Reducer-side
     * sort is still needed if the consumer requires ordered data.</p>
     */
    APPEND,

    /**
     * Sort-merge: committed batches from all mappers for the same partition are
     * merge-sorted by partition key into a globally-sorted sequential stream.
     * Eliminates reducer-side sort at the cost of server-side CPU.
     *
     * <p>Requires the shuffle service to understand the partition key schema.
     * Suitable for range-partitioned shuffles where downstream operators
     * (e.g., MergeJoin) require sorted input.</p>
     */
    SORT_MERGE
}
