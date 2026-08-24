package org.pangrova.coordinator.ai.embedding;

import java.time.Instant;

/**
 * Immutable representation of a row pending background embedding computation.
 *
 * <p>Created when a row is committed with NULL embedding in ASYNC mode,
 * and consumed by the {@code AsyncEmbeddingDrainer} for background processing.</p>
 *
 * @param tableName    fully qualified table name (catalog.schema.table)
 * @param rowId        unique identifier for the row
 * @param modelName    the embedding model to invoke
 * @param vectorColumn the target vector column to populate
 * @param sourceText   concatenated source column text for embedding
 * @param enqueuedAt   timestamp when the row was enqueued
 */
public record PendingEmbeddingRow(
        String tableName,
        String rowId,
        String modelName,
        String vectorColumn,
        String sourceText,
        Instant enqueuedAt) {

    public PendingEmbeddingRow {
        tableName = requireNotBlank(tableName, "tableName");
        rowId = requireNotBlank(rowId, "rowId");
        modelName = requireNotBlank(modelName, "modelName");
        vectorColumn = requireNotBlank(vectorColumn, "vectorColumn");
        if (sourceText == null) {
            throw new IllegalArgumentException("sourceText must not be null");
        }
        if (enqueuedAt == null) {
            throw new IllegalArgumentException("enqueuedAt required");
        }
    }

    private static String requireNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " required");
        }
        return value;
    }
}
