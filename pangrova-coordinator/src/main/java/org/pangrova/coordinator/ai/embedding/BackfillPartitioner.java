package org.pangrova.coordinator.ai.embedding;

import java.util.List;

/**
 * Partitions backfill work into discrete units for parallel Worker dispatch.
 *
 * <p>Each partition represents a batch of rows that can be independently
 * embedded and committed. Partition boundaries align with the configured
 * batch size.</p>
 */
public interface BackfillPartitioner {

    /**
     * Partitions the rows matching the filter into Worker-sized batches.
     *
     * @param tableName  the fully qualified table name
     * @param columnName the embedding column to backfill
     * @param filter     optional WHERE clause filter (null means all NULL rows for the column)
     * @param snapshotId the snapshot to scan at (snapshot isolation)
     * @param batchSize  maximum rows per partition
     * @return ordered list of partitions to process
     */
    List<BackfillPartition> partition(String tableName, String columnName,
                                     String filter, long snapshotId, int batchSize);
}
