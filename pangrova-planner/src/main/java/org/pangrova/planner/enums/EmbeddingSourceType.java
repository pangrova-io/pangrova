package org.pangrova.planner.enums;

/**
 * The type of source data used for embedding computation.
 */
public enum EmbeddingSourceType {
    /** Text sources are concatenated and embedded with DOCUMENT purpose. */
    TEXT,
    /** A FILE source is embedded with DOCUMENT purpose through the same batch contract. */
    FILE
}
