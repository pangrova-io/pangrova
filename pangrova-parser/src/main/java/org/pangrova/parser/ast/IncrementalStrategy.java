package org.pangrova.parser.ast;

/**
 * User-selected incremental refresh strategy.
 *
 * <p>{@link #AUTO} asks the kernel to select among registered strategies for
 * the planned definition; every other value pins one strategy and fails closed
 * when the definition or connector cannot prove it.
 */
public enum IncrementalStrategy {
    AUTO,
    APPEND_ONLY,
    PARTITION_REPLACE,
    AFFECTED_KEY,
    CDC
}
