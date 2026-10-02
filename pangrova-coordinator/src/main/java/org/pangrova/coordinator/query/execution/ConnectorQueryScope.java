package org.pangrova.coordinator.query.execution;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.SessionContextConverter;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.function.Consumer;

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

    public ConnectorQueryScope(
            CoordinatorSessionContext context, CatalogRegistry catalogRegistry,
            List<SplitSourceDescriptor> sources, Consumer<ConnectorMetadata> registrar) {
        checkNotNull(context, "context");
        checkNotNull(registrar, "registrar");
        if (catalogRegistry == null) {
            return;
        }
        var catalogs = new LinkedHashMap<String, SplitSourceDescriptor>();
        for (var source : sources) {
            catalogs.putIfAbsent(source.catalogName(), source);
        }
        for (var source : catalogs.values()) {
            var session = SessionContextConverter.toConnectorSession(
                    context, source.catalogName(), source.schemaName());
            var metadata = catalogRegistry.getCatalog(source.catalogName()).getMetadata(session);
            // Register before beginning so a partially successful begin is cleaned
            // with exactly this authenticated metadata instance on query failure.
            registrar.accept(metadata);
            metadata.beginQuery(context.queryId());
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
