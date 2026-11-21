package org.pangrova.parser.api.enums;

/**
 * Embedding computation mode.
 */
public enum EmbeddingMode {
    /** Compute embedding synchronously before DML commits. */
    SYNC,
    /** Commit row with NULL embedding, compute asynchronously in background. */
    ASYNC
}
