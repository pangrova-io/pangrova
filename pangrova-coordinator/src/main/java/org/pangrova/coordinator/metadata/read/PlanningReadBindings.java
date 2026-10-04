package org.pangrova.coordinator.metadata.read;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/** Query-local owner of immutable read bindings shared by schema and cost-statistic lookup. */
@org.pangrova.common.concurrent.ThreadSafe
public final class PlanningReadBindings {
    private final CatalogRegistry catalogs;
    private final ConnectorSession authenticatedSession;
    private final ConcurrentHashMap<TableKey, CompletableFuture<Optional<ConnectorReadBinding>>> bindings =
            new ConcurrentHashMap<>();

    /** Creates one binding owner for an authenticated compilation, not a cross-query live cache. */
    public PlanningReadBindings(CatalogRegistry catalogs, ConnectorSession session) {
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.authenticatedSession = PlanningConnectorSession.bind(session,
                session.getCatalog().orElse(null), session.getSchema().orElse(null));
    }

    /** Resolves once; the winning owner performs actual connector I/O without a cache lock. */
    public Optional<ConnectorReadBinding> resolve(String catalog, String schema, String table) {
        var key = new TableKey(catalog, schema, table);
        var proposed = new CompletableFuture<Optional<ConnectorReadBinding>>();
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
            return selected.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            if (e.getCause() instanceof Error cause) throw cause;
            throw e;
        }
    }

    private Optional<ConnectorReadBinding> load(TableKey key) {
        var session = PlanningConnectorSession.bind(authenticatedSession, key.catalog(), key.schema());
        if (session.getQueryAuthority().isPresent()) {
            try (var operation = session.getQueryAuthority().orElseThrow().enterOperation()) {
                return loadAdmitted(key, session);
            }
        }
        return loadAdmitted(key, session);
    }

    private Optional<ConnectorReadBinding> loadAdmitted(TableKey key, ConnectorSession session) {
        var metadata = session.getQueryAuthority().map(authority -> authority.beginMetadata(key.catalog(),
                () -> catalogs.getCatalog(key.catalog()).getMetadata(session)))
                .orElseGet(() -> catalogs.getCatalog(key.catalog()).getMetadata(session));
        var handle = metadata.getTableHandle(session, new SchemaTableName(key.schema(), key.table()));
        if (handle == null) return Optional.empty();
        var snapshot = PlanningSnapshotSelectors.snapshot(session, key.catalog(), key.schema(), key.table());
        if (snapshot.isPresent()) handle = handle.withTimeTravelSnapshot(snapshot.getAsLong());
        var binding = metadata.bindRead(session, handle);
        if (snapshot.isPresent() && (binding.snapshotId().isEmpty()
                || binding.snapshotId().getAsLong() != snapshot.getAsLong())) {
            throw new IllegalStateException("Connector did not bind the explicit source snapshot");
        }
        if (snapshot.isPresent()) binding.declaration().requireExactDeclaration();
        if (!binding.declaration().metadata().table().equals(new SchemaTableName(key.schema(), key.table()))) {
            throw new IllegalStateException("Connector read binding resolved another table");
        }
        return Optional.of(binding);
    }

    private record TableKey(String catalog, String schema, String table) {}
}
