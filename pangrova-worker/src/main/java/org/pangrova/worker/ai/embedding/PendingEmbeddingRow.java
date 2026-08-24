package org.pangrova.worker.ai.embedding;

import java.util.List;
import java.util.Map;

/**
 * Represents a single row pending asynchronous embedding computation.
 *
 * <p>Produced by the queue coordinator when rows are committed with NULL embeddings
 * in ASYNC mode. The drainer processes these rows in batches.</p>
 *
 * @param rowId          unique identifier for the row (table-scoped)
 * @param tableName      fully qualified table name
 * @param vectorColumn   target vector column name
 * @param modelName      registered embedding model name
 * @param sourceColumns  ordered list of source columns for embedding input
 * @param sourceValues   map of source column name to value for embedding input
 * @param attemptCount   number of previous failed attempts for this row (starts at 0)
 */
public record PendingEmbeddingRow(
        String rowId,
        String tableName,
        String vectorColumn,
        String modelName,
        List<String> sourceColumns,
        Map<String, String> sourceValues,
        int attemptCount) {

    public PendingEmbeddingRow {
        validateIdentifiers(rowId, tableName, vectorColumn);
        validateModelAndColumns(modelName, sourceColumns);
        sourceColumns = List.copyOf(sourceColumns);
        if (sourceValues == null) {
            throw new IllegalArgumentException("sourceValues must not be null");
        }
        sourceValues = Map.copyOf(sourceValues);
        if (attemptCount < 0) {
            throw new IllegalArgumentException("attemptCount must not be negative");
        }
    }

    private static void validateIdentifiers(String rowId, String tableName, String vectorColumn) {
        if (rowId == null || rowId.isBlank()) {
            throw new IllegalArgumentException("rowId must not be blank");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        if (vectorColumn == null || vectorColumn.isBlank()) {
            throw new IllegalArgumentException("vectorColumn must not be blank");
        }
    }

    private static void validateModelAndColumns(String modelName, List<String> sourceColumns) {
        if (modelName == null || modelName.isBlank()) {
            throw new IllegalArgumentException("modelName must not be blank");
        }
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            throw new IllegalArgumentException("sourceColumns must not be empty");
        }
    }
}
