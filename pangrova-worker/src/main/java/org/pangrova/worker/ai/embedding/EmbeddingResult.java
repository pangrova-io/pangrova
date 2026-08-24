package org.pangrova.worker.ai.embedding;

/**
 * Holds the computed embedding for a single row after async processing.
 *
 * <p>Produced by the drainer after a successful call to the embedding provider.
 * Committed back to the queue coordinator to persist the vector into the table.</p>
 *
 * @param rowId        unique identifier for the row (matches {@link PendingEmbeddingRow#rowId()})
 * @param tableName    fully qualified table name
 * @param vectorColumn target vector column name
 * @param embedding    the computed dense embedding vector
 */
public record EmbeddingResult(
        String rowId,
        String tableName,
        String vectorColumn,
        float[] embedding) {

    public EmbeddingResult {
        validateIdentifiers(rowId, tableName, vectorColumn);
        if (embedding == null || embedding.length == 0) {
            throw new IllegalArgumentException("embedding must not be null or empty");
        }
        embedding = embedding.clone();
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

    /**
     * Returns a defensive copy of the embedding vector.
     *
     * @return copy of the embedding array
     */
    @Override
    public float[] embedding() {
        return embedding.clone();
    }
}
