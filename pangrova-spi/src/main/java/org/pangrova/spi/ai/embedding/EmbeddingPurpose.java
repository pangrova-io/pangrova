package org.pangrova.spi.ai.embedding;

import org.pangrova.spi.annotation.Experimental;

/** Semantic purpose of an embedding, independent of physical batch cardinality. */
@Experimental
public enum EmbeddingPurpose {
    /** Embed the original input without a retrieval-specific transformation. */
    RAW,
    /** Embed a retrieval query using the exact model's query transformation. */
    QUERY,
    /** Embed stored content using the exact model's document transformation. */
    DOCUMENT
}
