package org.pangrova.coordinator.query.execution.dispatch.listener;

import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionEvent;
import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionListener;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Invalidates query result cache entries after table mutations.
 * Priority 20 — runs after schema refresh.
 */
public final class CacheInvalidationListener implements StatementCompletionListener {
    private static final int INVALIDATION_PRIORITY = 20;

    private final QueryCacheService cacheService;

    public CacheInvalidationListener(QueryCacheService cacheService) {
        this.cacheService = checkNotNull(cacheService, "cacheService");
    }

    @Override
    public void onCompleted(StatementCompletionEvent event) {
        StatementCategory category = event.result().category();
        if (isSchemaChangingCategory(category)) {
            cacheService.invalidateAll();
        }
        String table = event.mutatedTable();
        if (table != null) {
            cacheService.invalidateByTable(table);
        }
    }

    /**
     * Returns true for categories that change schema metadata and require
     * full cache invalidation. DDL, dynamic table, index, clustering, and
     * stream operations all modify schema state that may affect cached
     * query plans or results.
     */
    private static boolean isSchemaChangingCategory(StatementCategory category) {
        return category == StatementCategory.DDL
                || category == StatementCategory.DYNAMIC_TABLE
                || category == StatementCategory.INDEX
                || category == StatementCategory.CLUSTERING
                || category == StatementCategory.STREAM;
    }

    @Override
    public int priority() {
        return INVALIDATION_PRIORITY;
    }
}
