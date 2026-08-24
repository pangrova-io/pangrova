package org.pangrova.planner.embedding;

import java.util.List;

/**
 * Describes an auto-embedding column definition on a table.
 *
 * <p>Populated from catalog metadata when a column is declared with
 * {@code GENERATED ALWAYS AS (ai_embed('model', col1, col2)) EMBEDDING MODE SYNC|ASYNC}.</p>
 *
 * @param vectorColumn   name of the VECTOR column that stores embeddings
 * @param modelName      registered embedding model name
 * @param sourceColumns  ordered list of source columns whose values are concatenated for embedding
 * @param mode           embedding computation mode (SYNC or ASYNC)
 * @param dimension      output vector dimensionality (from model descriptor)
 * @param sourceType     the type of source data (TEXT for text columns, FILE for FILE columns)
 */
public record AutoEmbeddingColumnDescriptor(
        String vectorColumn,
        String modelName,
        List<String> sourceColumns,
        EmbeddingMode mode,
        int dimension,
        SourceType sourceType) {

    /**
     * Canonical constructor with sourceType defaulting to TEXT for backward compatibility.
     */
    public AutoEmbeddingColumnDescriptor(
            String vectorColumn,
            String modelName,
            List<String> sourceColumns,
            EmbeddingMode mode,
            int dimension) {
        this(vectorColumn, modelName, sourceColumns, mode, dimension, SourceType.TEXT);
    }

    public AutoEmbeddingColumnDescriptor {
        if (vectorColumn == null || vectorColumn.isBlank()) {
            throw new IllegalArgumentException("vectorColumn must not be blank");
        }
        if (modelName == null || modelName.isBlank()) {
            throw new IllegalArgumentException("modelName must not be blank");
        }
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            throw new IllegalArgumentException("sourceColumns must not be empty");
        }
        sourceColumns = List.copyOf(sourceColumns);
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (sourceType == null) {
            sourceType = SourceType.TEXT;
        }
    }

    /**
     * Embedding computation mode.
     */
    public enum EmbeddingMode {
        /** Compute embedding synchronously before DML commits. */
        SYNC,
        /** Commit with NULL embedding, enqueue for background computation. */
        ASYNC
    }

    /**
     * The type of source data used for embedding computation.
     */
    public enum SourceType {
        /** Source columns contain text data — concatenated and embedded via embedBatch(texts). */
        TEXT,
        /** Source column is a FILE (Claim-Check struct) — embedded via embedBlob(refs). */
        FILE
    }
}
