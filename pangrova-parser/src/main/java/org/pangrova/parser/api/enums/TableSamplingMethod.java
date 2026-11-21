package org.pangrova.parser.api.enums;

/**
 * Sampling method for TABLESAMPLE clause.
 */
public enum TableSamplingMethod {
    /** Row-level sampling: each row independently included with given probability. */
    BERNOULLI,
    /** Block-level sampling: entire blocks selected with given probability. */
    SYSTEM
}
