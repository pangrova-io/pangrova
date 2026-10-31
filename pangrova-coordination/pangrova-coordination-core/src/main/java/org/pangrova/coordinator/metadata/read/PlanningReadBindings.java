package org.pangrova.coordinator.metadata.read;

import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadataScope;
import org.pangrova.connector.spi.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/** Query-local owner of immutable read bindings shared by schema and cost-statistic lookup. */
@org.pangrova.common.concurrent.ThreadSafe
public final class PlanningReadBindings {
    private final CatalogRegistry catalogs;
    private final ConnectorSession authenticatedSession;
    private final ConcurrentHashMap<TableKey, CompletableFuture<AdmittedRead>> bindings =
            new ConcurrentHashMap<>();

    /** Creates one binding owner for an authenticated compilation, not a cross-query live cache. */
    public PlanningReadBindings(CatalogRegistry catalogs, ConnectorSession session) {
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.authenticatedSession = PlanningConnectorSession.bind(session,
                session.getCatalog().orElse(null), session.getSchema().orElse(null));
    }

    /** Resolves once; the winning owner performs actual connector I/O without a cache lock. */
    public Optional<ConnectorReadBinding> resolve(String catalog, String schema, String table) {
        var control = catalogs.tableControl();
        if (control.enabled()) {
            control.find(catalog, schema, table)
                    .ifPresent(record -> control.requireCurrentCatalog(record, "SQL read binding"));
        }
        var key = new TableKey(catalog, schema, table);
        var proposed = new CompletableFuture<AdmittedRead>();
        var existing = bindings.putIfAbsent(key, proposed);
        var selected = existing == null ? proposed : existing;
        if (existing == null) {
            try {
                proposed.complete(load(key));
            } catch (RuntimeException e) {
                proposed.completeExceptionally(new MetadataResolutionException(catalog, schema, table, e));
            } catch (Error e) {
                proposed.completeExceptionally(e);
                throw e;
            }
        }
        try {
            var admitted = selected.join();
            requireCurrentOwnership(key, admitted.owner());
            return admitted.binding();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            if (e.getCause() instanceof Error cause) throw cause;
            throw e;
        }
    }

    private AdmittedRead load(TableKey key) {
        var session = PlanningConnectorSession.bind(authenticatedSession, key.catalog(), key.schema());
        if (session.getQueryAuthority().isPresent()) {
            try (var operation = session.getQueryAuthority().orElseThrow().enterOperation()) {
                return loadAdmitted(key, session);
            }
        }
        return loadAdmitted(key, session);
    }

    private AdmittedRead loadAdmitted(TableKey key, ConnectorSession session) {
        var metadata = session.getQueryAuthority().map(ConnectorMetadataScope::require)
                .map(authority -> authority.beginMetadata(key.catalog(),
                    () -> catalogs.getCatalog(key.catalog()).getMetadata(session)))
                .orElseGet(() -> catalogs.getCatalog(key.catalog()).getMetadata(session));
        var control = catalogs.tableControl();
        var controlled = control.enabled() ? control.find(key.catalog(), key.schema(), key.table())
                : Optional.<TableControlRecord>empty();
        var handle = metadata.getTableHandle(session, new SchemaTableName(key.schema(), key.table()));
        if (handle == null) return new AdmittedRead(Optional.empty(), controlled);
        var liveHandle = handle;
        controlled.ifPresent(record -> control.requireReadIdentity(
                record, metadata.getTableIdentity(session, liveHandle), "SQL read binding"));
        var snapshot = PlanningSnapshotSelectors.snapshot(session, key.catalog(), key.schema(), key.table());
        var range = PlanningIncrementalSelectors.range(session, key.catalog(), key.schema(), key.table());
        if (snapshot.isPresent() && range.isPresent()) {
            throw new IllegalStateException("A source cannot bind both an exact snapshot and an incremental range");
        }
        if (snapshot.isPresent()) handle = handle.withTimeTravelSnapshot(snapshot.getAsLong());
        if (range.isPresent()) handle = handle.withIncrementalRange(range.orElseThrow());
        var binding = metadata.bindRead(session, handle);
        controlled.ifPresent(record -> control.requireReadIdentity(
                record, metadata.getTableIdentity(session, liveHandle), "SQL read binding"));
        requireCurrentOwnership(key, controlled);
        if (snapshot.isPresent() && (binding.snapshotId().isEmpty()
                || binding.snapshotId().getAsLong() != snapshot.getAsLong())) {
            throw new IllegalStateException("Connector did not bind the explicit source snapshot");
        }
        if (range.isPresent() && !range.equals(binding.handle().incrementalRange())) {
            throw new IllegalStateException("Connector did not preserve the incremental source range");
        }
        if (snapshot.isPresent() || range.isPresent()) binding.declaration().requireExactDeclaration();
        if (!binding.declaration().metadata().table().equals(new SchemaTableName(key.schema(), key.table()))) {
            throw new IllegalStateException("Connector read binding resolved another table");
        }
        return new AdmittedRead(Optional.of(binding), controlled);
    }

    private void requireCurrentOwnership(TableKey key, Optional<TableControlRecord> expected) {
        var control = catalogs.tableControl();
        var current = control.enabled() ? control.find(key.catalog(), key.schema(), key.table())
                : Optional.<TableControlRecord>empty();
        if (!expected.map(TableControlRecord::tableId).equals(current.map(TableControlRecord::tableId))) {
            throw new org.pangrova.security.api.AccessDeniedException(
                    "SQL table ownership changed during read binding; resolve the table again");
        }
        expected.ifPresent(record -> control.requireReadOwner(record, "Cached SQL read binding"));
    }

    private record AdmittedRead(Optional<ConnectorReadBinding> binding, Optional<TableControlRecord> owner) {}

    private record TableKey(String catalog, String schema, String table) {}
}
