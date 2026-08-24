package org.pangrova.coordinator.ai.embedding;

import java.util.Optional;

/**
 * Persistence store for backfill checkpoints.
 *
 * <p>Enables checkpoint-based recovery: after each batch commit, the coordinator
 * saves a checkpoint. On crash recovery, the coordinator loads the last checkpoint
 * and resumes from the next partition.</p>
 *
 * <p>Thread safety: implementations must be safe for concurrent use.</p>
 */
public interface BackfillCheckpointStore {

    /**
     * Persists a checkpoint after a successful batch commit.
     *
     * @param checkpoint the checkpoint to save (overwrites any previous checkpoint
     *                   for the same table/column)
     */
    void save(BackfillCheckpoint checkpoint);

    /**
     * Loads the most recent checkpoint for a table/column pair.
     *
     * @param tableName  the table name
     * @param columnName the column name
     * @return the checkpoint if one exists, empty otherwise
     */
    Optional<BackfillCheckpoint> load(String tableName, String columnName);

    /**
     * Removes the checkpoint for a completed or cancelled backfill.
     *
     * @param tableName  the table name
     * @param columnName the column name
     */
    void remove(String tableName, String columnName);
}
