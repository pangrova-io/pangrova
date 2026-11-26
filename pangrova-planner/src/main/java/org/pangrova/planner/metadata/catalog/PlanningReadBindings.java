package org.pangrova.planner.metadata.catalog;

import org.pangrova.metadata.api.catalog.CatalogReadAccess;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.metadata.api.exception.MetadataResolutionException;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorReadDiscovery;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.planner.statistics.StatisticsCollector.ConnectorStatisticsProvider;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.statistics.CurrentTableStatistics;
import org.pangrova.table.api.statistics.TableControlStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Query-local owner of immutable read bindings shared by schema and cost-statistic lookup. */
@org.pangrova.common.concurrent.ThreadSafe
public final class PlanningReadBindings implements ConnectorStatisticsProvider {
    private final CatalogReadAccess catalogs;
    private final ConnectorSession authenticatedSession;
    private final CurrentTableStatistics persistedStatistics;
    private final ConcurrentHashMap<QualifiedTableName, CompletableFuture<AdmittedRead>> bindings =
            new ConcurrentHashMap<>();

    /** Creates one binding owner for an authenticated compilation, not a cross-query live cache. */
    public PlanningReadBindings(CatalogReadAccess catalogs, ConnectorSession session) {
        this(catalogs, session, null);
    }

    /** Creates a compilation owner with access to versioned ANALYZE results. */
    public PlanningReadBindings(CatalogReadAccess catalogs, ConnectorSession session,
            CurrentTableStatistics persistedStatistics) {
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.authenticatedSession = PlanningConnectorSession.bind(session,
                session.getCatalog().orElse(null), session.getSchema().orElse(null));
        this.persistedStatistics = persistedStatistics;
    }

    /** Resolves once; the winning owner performs actual connector I/O without a cache lock. */
    public Optional<ConnectorReadBinding> resolve(String catalog, String schema, String table) {
        return resolve(new QualifiedTableName(catalog, schema, table), null);
    }

    Optional<ConnectorReadBinding> resolve(QualifiedTableName table, ConnectorReadDiscovery discovery) {
        var admitted = resolveAdmitted(table, discovery);
        requireCurrentOwnership(table, admitted.owner());
        return admitted.binding();
    }

    /** Discovers a named source without opening its schema or collecting its statistics. */
    MetadataResolvedTable discover(QualifiedTableName table) {
        return discover(table, false);
    }

    MetadataResolvedTable discoverExactSpelling(QualifiedTableName table) {
        return discover(table, true);
    }

    List<SchemaTableName> listTableNames(String catalog, String schema) {
        return withSession(catalog, schema, session -> catalogs.metadataForQuery(catalog, session)
                .listTables(session, Optional.of(schema)));
    }

    List<String> listSchemaNames(String catalog) {
        return withSession(catalog, null, session -> catalogs.metadataForQuery(catalog, session)
                .listSchemaNames(session));
    }

    boolean hasExactSchemaName(String catalog, String schema) {
        return withSession(catalog, null, session -> catalogs.metadataForQuery(catalog, session)
                .hasExactSchemaName(session, schema));
    }

    private MetadataResolvedTable discover(QualifiedTableName table, boolean requireExactSpelling) {
        if (!requireExactSpelling) {
            var admitted = bindings.get(table);
            if (admitted != null) return discoveredFromBinding(table, admitted);
        }
        return withSession(table, session -> {
            var metadata = catalogs.metadataForQuery(table.catalog(), session);
            if (requireExactSpelling) {
                if (!metadata.supportsExactTableNameLookup()) return null;
                var admitted = bindings.get(table);
                if (admitted != null) return discoveredFromBinding(table, admitted);
            }
            var discovery = metadata.discoverRead(session, new SchemaTableName(table.schema(), table.table()));
            return discovery == null ? null : MetadataResolvedTable.fromReadBinding(this, table, discovery);
        });
    }

    private MetadataResolvedTable discoveredFromBinding(QualifiedTableName table,
            CompletableFuture<AdmittedRead> admitted) {
        return await(admitted).binding().isPresent()
                ? MetadataResolvedTable.fromReadBinding(this, table, null) : null;
    }

    /** Supplies the immutable declaration and cardinality for this compilation. */
    ConnectorReadBinding selectedRead(QualifiedTableName table, ConnectorReadDiscovery discovery) {
        return resolveAdmitted(table, discovery).binding()
                .orElseThrow(() -> new IllegalStateException("Selected table disappeared"));
    }

    /** Memoizes optimizer estimates for this compilation, including absent statistics. */
    @Override
    public TableStatistics statistics(QualifiedTableName table) {
        var admitted = resolveAdmitted(table, null);
        var memo = admitted.statistics();
        var existing = memo.get();
        if (existing != null) return await(existing);
        var proposed = new CompletableFuture<TableStatistics>();
        existing = memo.compareAndExchange(null, proposed);
        if (existing != null) return await(existing);
        try {
            // These estimates describe the previously admitted immutable source. Schema
            // reuse and split enumeration enforce their own live ownership checks.
            proposed.complete(loadStatistics(admitted));
        } catch (RuntimeException | Error failure) {
            proposed.completeExceptionally(failure);
            throw failure;
        }
        return proposed.join();
    }

    /** Claims the exact sources requested together by a cost estimate. */
    @Override
    public void prepareStatistics(Set<QualifiedTableName> tables) {
        if (persistedStatistics == null) return;
        var pending = new ArrayList<PendingEstimate>();
        try {
            for (var table : tables) {
                var admitted = resolveAdmitted(table, null);
                var owner = statisticsOwner(admitted);
                if (owner == null || admitted.statistics().get() != null) continue;
                var future = new CompletableFuture<TableStatistics>();
                if (admitted.statistics().compareAndSet(null, future)) {
                    pending.add(new PendingEstimate(admitted, owner, future));
                }
            }
            if (pending.isEmpty()) return;
            var owners = pending.stream().map(PendingEstimate::owner)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            var estimates = persistedStatistics.freshBatch(owners);
            for (var request : pending) {
                var estimate = estimates.get(request.owner());
                request.future().complete(selectedEstimate(request.admitted(), request.owner(), estimate));
            }
        } catch (RuntimeException | Error failure) {
            pending.forEach(request -> request.future().completeExceptionally(failure));
            throw failure;
        }
    }

    private record PendingEstimate(AdmittedRead admitted, TableControlRecord owner,
            CompletableFuture<TableStatistics> future) { }

    private TableStatistics loadStatistics(AdmittedRead admitted) {
        var owner = statisticsOwner(admitted);
        var estimate = persistedStatistics == null || owner == null ? null
                : persistedStatistics.freshBatch(Set.of(owner)).get(owner);
        return selectedEstimate(admitted, owner, estimate);
    }

    private static TableStatistics selectedEstimate(AdmittedRead admitted, TableControlRecord owner,
            TableControlStatistics estimate) {
        if (estimate != null && estimate.tableId().equals(owner.tableId())
                && estimate.matchesVersion(owner.versionToken())) {
            return estimate.statistics();
        }
        return admitted.binding().map(ConnectorReadBinding::statistics).orElse(TableStatistics.UNKNOWN);
    }

    private static TableControlRecord statisticsOwner(AdmittedRead admitted) {
        var owner = admitted.owner().orElse(null);
        var version = admitted.binding().flatMap(ConnectorReadBinding::statisticsVersion).orElse(null);
        return owner != null && owner.state() == TableControlState.ACTIVE && version != null
                && version.format().equals(owner.format())
                && version.token().equals(owner.versionToken()) ? owner : null;
    }

    private AdmittedRead resolveAdmitted(QualifiedTableName key, ConnectorReadDiscovery discovery) {
        var cached = bindings.get(key);
        if (cached != null) return await(cached);
        var proposed = new CompletableFuture<AdmittedRead>();
        var existing = bindings.putIfAbsent(key, proposed);
        if (existing != null) return await(existing);
        try {
            proposed.complete(withSession(key, session -> loadAdmitted(key, session, discovery)));
        } catch (RuntimeException e) {
            proposed.completeExceptionally(
                    new MetadataResolutionException(key.catalog(), key.schema(), key.table(), e));
        } catch (Error e) {
            proposed.completeExceptionally(e);
            throw e;
        }
        return await(proposed);
    }

    private static <T> T await(CompletableFuture<T> selected) {
        try {
            return selected.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            if (e.getCause() instanceof Error cause) throw cause;
            throw e;
        }
    }

    private <T> T withSession(QualifiedTableName key, Function<ConnectorSession, T> action) {
        return withSession(key.catalog(), key.schema(), action);
    }

    private <T> T withSession(String catalog, String schema, Function<ConnectorSession, T> action) {
        var session = PlanningConnectorSession.bind(authenticatedSession, catalog, schema);
        if (session.getQueryAuthority().isPresent()) {
            try (var operation = session.getQueryAuthority().orElseThrow().enterOperation()) {
                return action.apply(session);
            }
        }
        return action.apply(session);
    }

    private AdmittedRead loadAdmitted(QualifiedTableName key,
            ConnectorSession session, ConnectorReadDiscovery discovery) {
        var control = catalogs.tableControl();
        var controlled = control.enabled() ? control.findForRead(key, "SQL read binding")
                : Optional.<TableControlRecord>empty();
        var metadata = catalogs.metadataForQuery(key.catalog(), session);
        var discovered = discovery != null && discovery.metadata() == metadata ? discovery
                : metadata.discoverRead(session, new SchemaTableName(key.schema(), key.table()));
        if (discovered == null) return new AdmittedRead(Optional.empty(), controlled);
        var handle = discovered.handle();
        var liveHandle = handle;
        var snapshot = PlanningSnapshotSelectors.snapshot(session, key.catalog(), key.schema(), key.table());
        var range = PlanningIncrementalSelectors.range(session, key.catalog(), key.schema(), key.table());
        if (snapshot.isPresent() && range.isPresent()) {
            throw new IllegalStateException("A source cannot bind both an exact snapshot and an incremental range");
        }
        if (snapshot.isPresent()) handle = handle.withTimeTravelSnapshot(Long.toString(snapshot.getAsLong()));
        if (range.isPresent()) handle = handle.withIncrementalRange(range.orElseThrow());
        ConnectorReadBinding binding;
        if (controlled.isPresent()) {
            var record = controlled.orElseThrow();
            binding = discovered.binder().bind(session, handle,
                    identity -> control.requireReadIdentity(record, identity, "SQL read binding"));
            control.requireReadIdentity(record, binding.schemaIdentity().map(ConnectorReadSchemaIdentity::tableIdentity)
                    .or(() -> metadata.getTableIdentity(session, liveHandle)), "SQL read binding");
        } else {
            binding = discovered.binder().bind(session, handle, null);
        }
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

    private void requireCurrentOwnership(QualifiedTableName key, Optional<TableControlRecord> expected) {
        var control = catalogs.tableControl();
        if (expected.isPresent()) {
            control.requireReadOwner(expected.orElseThrow(), "Cached SQL read binding");
        } else if (control.enabled() && control.find(key.catalog(), key.schema(), key.table()).isPresent()) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "SQL table ownership changed during read binding; resolve the table again");
        }
    }

    private record AdmittedRead(Optional<ConnectorReadBinding> binding, Optional<TableControlRecord> owner,
            AtomicReference<CompletableFuture<TableStatistics>> statistics) {
        private AdmittedRead(Optional<ConnectorReadBinding> binding, Optional<TableControlRecord> owner) {
            this(binding, owner, new AtomicReference<>());
        }
    }
}
