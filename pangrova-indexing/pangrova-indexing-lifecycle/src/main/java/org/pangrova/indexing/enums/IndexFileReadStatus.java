package org.pangrova.indexing.enums;

/** Outcome of opening index data. */
public enum IndexFileReadStatus {
    /** File existed and was successfully read. */
    PRESENT,
    /** File does not exist; treat as "no index", not a failure. */
    MISSING,
    /** File exists but could not be read (I/O error, oversized, etc.); a failure. */
    ERROR
}
