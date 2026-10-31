package org.pangrova.connector.spi.index;

/**
 * Result of evaluating an index against a predicate for a given data file.
 *
 * <p>The evaluation contract guarantees no false negatives: if a data file
 * contains rows matching the predicate, the result is never {@code SKIP}.
 */
public enum IndexEvalResult {

    /** The data file provably contains no matching rows and can be skipped. */
    SKIP,

    /** The data file may contain matching rows and must be read. */
    KEEP,

    /** The index cannot determine whether the data file matches; treat as KEEP. */
    UNKNOWN
}
