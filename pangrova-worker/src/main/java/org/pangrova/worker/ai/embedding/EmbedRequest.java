package org.pangrova.worker.ai.embedding;

import java.util.List;

/**
 * Describes a single embedding computation request for one auto-embedding column.
 *
 * <p>Produced by deserializing the planner's injection plan on the worker side.
 * Each request targets one vector column and specifies the model and source columns
 * to concatenate for embedding input.</p>
 *
 * @param vectorColumn  target vector column name
 * @param modelName     registered embedding model name
 * @param sourceColumns ordered list of source columns whose values form the embedding input
 * @param dimension     expected output vector dimensionality
 * @param sourceType    type of source data (TEXT or FILE); determines embedding path
 */
public record EmbedRequest(
        String vectorColumn,
        String modelName,
        List<String> sourceColumns,
        int dimension,
        SourceType sourceType) {

    /**
     * Backward-compatible constructor defaulting sourceType to TEXT.
     */
    public EmbedRequest(
            String vectorColumn,
            String modelName,
            List<String> sourceColumns,
            int dimension) {
        this(vectorColumn, modelName, sourceColumns, dimension, SourceType.TEXT);
    }

    public EmbedRequest {
        validateIdentifiers(vectorColumn, modelName);
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

    private static void validateIdentifiers(String vectorColumn, String modelName) {
        if (vectorColumn == null || vectorColumn.isBlank()) {
            throw new IllegalArgumentException("vectorColumn must not be blank");
        }
        if (modelName == null || modelName.isBlank()) {
            throw new IllegalArgumentException("modelName must not be blank");
        }
    }

    /**
     * Returns true if this request uses FILE (blob) source for embedding.
     */
    public boolean isFileSource() {
        return sourceType == SourceType.FILE;
    }

    /**
     * The type of source data used for embedding computation.
     */
    public enum SourceType {
        /** Text columns — concatenated and embedded via text embedding API. */
        TEXT,
        /** FILE column (Claim-Check struct) — embedded via blob embedding API. */
        FILE
    }
}
