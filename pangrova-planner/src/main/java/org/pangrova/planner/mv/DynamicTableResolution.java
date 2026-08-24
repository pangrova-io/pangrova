package org.pangrova.planner.mv;

import java.util.Objects;

/**
 * Resolution result for a Dynamic Table query.
 *
 * <p>When a query references a Dynamic Table, the planner resolves it to either:
 * <ul>
 *   <li>{@link DirectTarget} — DT is fully refreshed, query the materialized table directly</li>
 *   <li>{@link MergeOnRead} — DT has pending deltas, merge materialized + incremental via UNION ALL</li>
 * </ul>
 *
 * <p>Returned by {@link DynamicTableRewriter#resolve(String)} to inform
 * the planner whether a simple table swap or a UNION ALL rewrite is needed.</p>
 *
 * @see DynamicTableRewriter
 */
public sealed interface DynamicTableResolution
        permits DynamicTableResolution.DirectTarget,
                DynamicTableResolution.MergeOnRead {

    /**
     * DT fully refreshed — query the materialized table directly.
     *
     * @param tableName the physical materialized table name (e.g. {@code __dt_NAME})
     */
    record DirectTarget(String tableName) implements DynamicTableResolution {
        public DirectTarget {
            Objects.requireNonNull(tableName, "tableName must not be null");
        }
    }

    /**
     * DT has pending deltas — merge materialized data with incremental scan.
     *
     * <p>The planner generates a UNION ALL plan:
     * <pre>
     *   SELECT * FROM materializedTable WHERE ...   -- data up to lastRefreshSnapshot
     *   UNION ALL
     *   SELECT * FROM sourceTable WHERE ...         -- delta from lastRefreshSnapshot to currentSnapshot
     * </pre>
     *
     * @param materializedTable    physical materialized table ({@code __dt_NAME})
     * @param lastRefreshSnapshot  snapshot ID up to which the materialized table is current
     * @param sourceTable          original source table for incremental scan
     * @param currentSnapshot      latest snapshot ID of the source table
     */
    record MergeOnRead(
            String materializedTable,
            long lastRefreshSnapshot,
            String sourceTable,
            long currentSnapshot
    ) implements DynamicTableResolution {
        public MergeOnRead {
            Objects.requireNonNull(materializedTable, "materializedTable must not be null");
            Objects.requireNonNull(sourceTable, "sourceTable must not be null");
            if (lastRefreshSnapshot < 0) {
                throw new IllegalArgumentException("lastRefreshSnapshot must be non-negative");
            }
            if (currentSnapshot < lastRefreshSnapshot) {
                throw new IllegalArgumentException(
                        "currentSnapshot (%d) must be >= lastRefreshSnapshot (%d)"
                                .formatted(currentSnapshot, lastRefreshSnapshot));
            }
        }
    }
}
