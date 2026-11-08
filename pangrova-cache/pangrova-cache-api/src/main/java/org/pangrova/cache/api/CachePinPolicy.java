package org.pangrova.cache.api;

/**
 * Defines the cache pin policy for data associated with a table.
 *
 * <p>When a table is pinned via the coordinator warmup service, the pin policy
 * indicates how aggressively cached data for that table should be retained
 * in the native data cache. This is the
 * coordinator-side policy declaration; actual eviction-prevention enforcement
 * happens in the worker's native cache layer via the pin metadata propagated
 * in the heartbeat / task-submit path.</p>
 *
 * <p>Equivalent concept: Spark's {@code CACHE TABLE} / Trino's
 * {@code CACHE SELECT} combined with a retention hint.</p>
 *
 */
public enum CachePinPolicy {

    /**
     * No pinning — data may be evicted normally under cache pressure.
     * This is the default for all tables.
     */
    NONE,

    /**
     * Hot-table pin: requests that cached data for this table be retained
     * with elevated priority. Under memory pressure, pinned entries are
     * evicted last (after all unpinned entries have been considered).
     *
     * <p>This does NOT guarantee data stays cached indefinitely — if the
     * cache is full of pinned entries, the oldest-pinned entry will be
     * evicted. The semantics are "best-effort sticky" rather than hard
     * reservation, similar to Presto/Trino's cache-pinning semantics.</p>
     */
    HOT_TABLE,

    /**
     * Exclusive pin: reserves a fixed fraction of cache capacity for this
     * table's data. Reserved entries are never evicted until the pin is
     * explicitly removed.
     *
     * <p><b>Caution:</b> over-allocating exclusive pins can starve the
     * general cache and degrade overall query performance. This policy
     * should be used sparingly for genuinely critical dimension tables.</p>
     *
     * <p>Currently not enforced in the native layer — treated as
     * {@link #HOT_TABLE} at runtime. Defined here for forward
     * compatibility so the session-property surface does not need to
     * change when native support lands.</p>
     */
    EXCLUSIVE;

    /**
     * Parses a pin policy from a case-insensitive string.
     *
     * @param value the string representation (e.g. "hot_table", "NONE")
     * @return the matching policy
     * @throws IllegalArgumentException if the value does not match any policy
     */
    public static CachePinPolicy fromString(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        try {
            return valueOf(value.strip().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown cache pin policy: '" + value
                            + "'. Valid values: NONE, HOT_TABLE, EXCLUSIVE");
        }
    }
}
