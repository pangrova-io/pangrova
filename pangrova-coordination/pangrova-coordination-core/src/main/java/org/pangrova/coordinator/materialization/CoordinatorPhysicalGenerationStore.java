package org.pangrova.coordinator.materialization;

import org.pangrova.observability.spi.tracing.ExecutionTraceContextFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.port.PhysicalGenerationStore;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.security.api.identity.UserIdentity;

/**
 * Connector-backed physical generation lifecycle for garbage collection.
 *
 * <p>The store runs as the internal GC principal in the candidate table's
 * catalog and schema. Both operations are idempotent; a connector failure is
 * surfaced so the collector records the failed ledger attempt for retry.
 */
public final class CoordinatorPhysicalGenerationStore implements PhysicalGenerationStore {
    private final ExecutionTraceContextFactory traceContexts;

    private static final String PRINCIPAL = "system:materialization-gc";

    private final CatalogRegistry catalogs;

    public CoordinatorPhysicalGenerationStore(CatalogRegistry catalogs, ExecutionTraceContextFactory traceContexts) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
    }

    @Override
    public boolean exists(MaterializationId object, PhysicalTableRef table) {
        ConnectorSession session = session(object, table);
        ConnectorTableHandle handle = metadata(table, session)
                .getTableHandle(session, tableName(table));
        return handle != null;
    }

    @Override
    public void drop(MaterializationId object, PhysicalTableRef table) {
        ConnectorSession session = session(object, table);
        ConnectorMetadata metadata = metadata(table, session);
        ConnectorTableHandle handle = metadata.getTableHandle(session, tableName(table));
        if (handle != null) {
            metadata.dropTable(session, handle);
        }
    }

    private ConnectorMetadata metadata(PhysicalTableRef table, ConnectorSession session) {
        return catalogs.getCatalog(table.catalog()).getMetadata(session);
    }

    private ConnectorSession session(MaterializationId object, PhysicalTableRef table) {
        QuerySession context = new QuerySession(
                "mat-gc-" + UUID.randomUUID(),
                Map.of("catalog", table.catalog(), "schema", table.schema()),
                new UserIdentity(PRINCIPAL, Set.of(), Map.of(), Instant.now()),
                object.tenant().value(), java.util.Optional.empty(), traceContexts.capture());
        return SessionContextConverter.toConnectorSession(
                context, table.catalog(), table.schema());
    }

    private static SchemaTableName tableName(PhysicalTableRef table) {
        return new SchemaTableName(table.schema(), table.table());
    }
}
