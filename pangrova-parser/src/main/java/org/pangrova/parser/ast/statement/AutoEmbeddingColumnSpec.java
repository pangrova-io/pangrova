package org.pangrova.parser.ast.statement;

import java.util.List;

/**
 * Specification for an auto-embedding column declared via:
 * {@code GENERATED ALWAYS AS (ai_embed('model', col1, col2, ...)) EMBEDDING MODE SYNC|ASYNC}
 *
 * <p>Captures the embedding model name, source columns to embed, and the embedding mode
 * (synchronous or asynchronous). Used in both CREATE TABLE column definitions and
 * ALTER TABLE ADD COLUMN statements.</p>
 *
 * @param modelName     the registered embedding model name (e.g. 'bge_m3')
 * @param sourceColumns the source columns whose values are concatenated and embedded
 * @param mode          the embedding computation mode (SYNC or ASYNC)
 */
public record AutoEmbeddingColumnSpec(
        String modelName,
        List<String> sourceColumns,
        EmbeddingMode mode) {

    public AutoEmbeddingColumnSpec {
        if (modelName == null || modelName.isBlank()) {
            throw new IllegalArgumentException("modelName must not be blank");
        }
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            throw new IllegalArgumentException("sourceColumns must not be empty");
        }
        sourceColumns = List.copyOf(sourceColumns);
    }

    /**
     * Embedding computation mode.
     */
    public enum EmbeddingMode {
        /** Compute embedding synchronously before DML commits. */
        SYNC,
        /** Commit row with NULL embedding, compute asynchronously in background. */
        ASYNC
    }
}
