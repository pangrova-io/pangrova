package org.pangrova.coordinator.query.execution;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Notifies all registered connectors that a query has begun.
 *
 * <p>Pin lifecycle is bound to QueryId: {@code beginQuery} is called here at
 * dispatch time, and {@code cleanupQuery} is called by
 * {@link QueryOwnershipTracker#cleanupQuery(String)} when the query reaches a
 * terminal state (FINISHED, FAILED, or CANCELLED) via {@code QueryManager}.
 * This ensures snapshot pins are held for the entire query duration, not just
 * the dispatch phase.</p>
 *
 * <p>Implements {@link AutoCloseable} as a no-op close so existing
 * try-with-resources call sites compile without change. Cleanup is NOT
 * performed on close — it is deferred to the query terminal-state handler.</p>
 */
public final class ConnectorQueryScope implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorQueryScope.class);

    private final String queryId;
    private final CatalogRegistry catalogRegistry;

    public ConnectorQueryScope(String queryId, CatalogRegistry catalogRegistry) {
        this.queryId = checkNotNull(queryId, "queryId");
        this.catalogRegistry = catalogRegistry;
        notifyBeginQuery();
    }

    private void notifyBeginQuery() {
        if (catalogRegistry == null) {
            return;
        }
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                var session = new org.pangrova.coordinator.query.statement.StatementConnectorSession(
                        catalogName, "default");
                catalogRegistry.getCatalog(catalogName).getMetadata(session).beginQuery(queryId);
            } catch (Exception e) {
                LOG.debug("event=connector_begin_query_failed catalog={} queryId={} error={}",
                        catalogName, queryId, e.getMessage());
            }
        }
    }

    /**
     * No-op. Cleanup is handled by {@link QueryOwnershipTracker#cleanupQuery(String)}
     * when the query reaches a terminal state, ensuring snapshot pins are held
     * for the full query lifetime.
     */
    @Override
    public void close() {
        // Intentional no-op: cleanup is deferred to query terminal-state handler
        // (QueryManager.onAllTasksFinished / failQuery / cancelQuery) which calls
        // QueryDispatcher.cleanupQuery → QueryOwnershipTracker.cleanupQuery →
        // notifyConnectorsCleanupQuery. This ensures pins survive the entire
        // query execution, not just the dispatch phase.
    }
}

