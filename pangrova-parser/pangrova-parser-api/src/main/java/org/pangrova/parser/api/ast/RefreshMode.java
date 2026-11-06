package org.pangrova.parser.api.ast;

/**
 * Refresh mode for Dynamic Table incremental materialized views.
 *
 * <ul>
 *   <li>{@link #SNAPSHOT} — snapshot-diff based micro-batch refresh</li>
 *   <li>{@link #CDC} — CDC stream based real-time refresh</li>
 *   <li>{@link #AUTO} — system auto-selects (prefers CDC, falls back to SNAPSHOT)</li>
 * </ul>
 */
public enum RefreshMode {
    SNAPSHOT,
    FULL,
    CDC,
    AUTO,
    INCREMENTAL
}
