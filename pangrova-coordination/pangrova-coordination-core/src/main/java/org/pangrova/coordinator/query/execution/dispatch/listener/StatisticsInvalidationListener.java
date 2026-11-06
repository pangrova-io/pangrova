package org.pangrova.coordinator.query.execution.dispatch.listener;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionEvent;
import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionListener;
import org.pangrova.planner.core.statistics.StatisticsCollector;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Invalidates planner statistics cache entries after table mutations.
 * Priority 15 — runs after schema refresh (10) but before result cache
 * invalidation (20), ensuring the planner sees fresh statistics for
 * subsequent queries against mutated tables.
 *
 * <p>For DDL statements that change schema (CREATE/DROP/ALTER TABLE),
 * all statistics are invalidated because table structure changes may
 * make column statistics meaningless. For DML (INSERT, DELETE, MERGE)
 * and maintenance (COMPACT, VACUUM), only the affected table's
 * statistics are invalidated.</p>
 */
public final class StatisticsInvalidationListener implements StatementCompletionListener {
    private static final int INVALIDATION_PRIORITY = 15;

    private final StatisticsCollector statisticsCollector;

    public StatisticsInvalidationListener(StatisticsCollector statisticsCollector) {
        this.statisticsCollector = checkNotNull(statisticsCollector, "statisticsCollector");
    }

    @Override
    public void onCompleted(StatementCompletionEvent event) {
        StatementCategory category = event.result().category();
        if (isSchemaChangingCategory(category)) {
            // DDL changes table structure — invalidate all stats
            statisticsCollector.invalidateAll();
            return;
        }
        if (isDataChangingCategory(category)) {
            // DML/maintenance changes data — invalidate only the affected table
            QualifiedTableName table = event.mutatedTableIdentity();
            if (table != null) {
                statisticsCollector.invalidate(table);
            }
        }
    }

    /**
     * Categories that change schema structure, requiring full stats invalidation.
     */
    private static boolean isSchemaChangingCategory(StatementCategory category) {
        return category == StatementCategory.DDL
                || category == StatementCategory.DYNAMIC_TABLE
                || category == StatementCategory.INDEX
                || category == StatementCategory.CLUSTERING;
    }

    /**
     * Categories that change data without altering schema, requiring
     * per-table stats invalidation.
     */
    private static boolean isDataChangingCategory(StatementCategory category) {
        return category == StatementCategory.DML
                || category == StatementCategory.MAINTENANCE
                || category == StatementCategory.STREAM;
    }

    @Override
    public int priority() {
        return INVALIDATION_PRIORITY;
    }
}
