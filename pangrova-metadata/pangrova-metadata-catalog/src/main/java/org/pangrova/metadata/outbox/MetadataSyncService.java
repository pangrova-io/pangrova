package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.enums.MetadataChangeType;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.exception.MetadataSyncException;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.api.schema.SchemaValidationResult;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncConflictException;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncPendingException;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncOutcomeUnknownException;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionScopeException;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException;

/**
 * Orchestrates incremental and full metadata synchronization between external catalogs and the local store.
 *
 * <p>The persistent store owns durable invalidation in the same transaction as each
 * local projection mutation. Local cache invalidation follows a successful store call;
 * this service never publishes a second, independently committed invalidation.</p>
 *
 * <p>Sync reliability features:</p>
 * <ul>
 *   <li>Incremental sync with automatic fallback to full sync on failure</li>
 *   <li>Post-sync schema drift detection on a random 10% sample</li>
 *   <li>Push-based change subscription for connectors that support it</li>
 * </ul>
 */
public class MetadataSyncService {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataSyncService.class);
    private static final int MAX_RETRIES = 3;
    private static final double DRIFT_SAMPLE_RATE = 0.1;

    private final TableProjectionRepository tables;
    private final MetadataProjectionTransactions projections;
    private final MetadataSyncRepository synchronization;
    private final MetadataCache cache;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionValidator schemaEvolutionValidator;
    private final AtomicLong schemaDriftDetectedCount = new AtomicLong();

    /** Shares the persistent mutation authority, local cache and metadata event dispatcher. */
    public MetadataSyncService(TableProjectionRepository tables,
            MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization,
            MetadataCache cache, MetadataEventDispatcher eventDispatcher) {
        this.tables = java.util.Objects.requireNonNull(tables, "tables");
        this.projections = java.util.Objects.requireNonNull(projections, "projections");
        this.synchronization = java.util.Objects.requireNonNull(synchronization, "synchronization");
        this.cache = cache;
        this.eventDispatcher = eventDispatcher;
        this.schemaEvolutionValidator = new SchemaEvolutionValidator();
    }

    /** Applies the source batch for the exact deployment that created the adapter. */
    public SyncResult incrementalSync(CatalogDescriptor deployment, FederatedCatalog adapter) {
        Instant start = Instant.now();
        CatalogDeploymentIdentity scope = scope(deployment);
        checkCancellation();
        long epoch = beginObservation(scope);
        Optional<Instant> checkpoint = synchronization.loadCheckpoint(scope);
        var batch = adapter.readChangesSince(checkpoint.orElse(Instant.EPOCH));
        if (batch.completeThrough().isPresent()
                && batch.completeThrough().get().isBefore(checkpoint.orElse(Instant.EPOCH))) {
            throw new IllegalArgumentException("Source coverage precedes the requested checkpoint");
        }
        validateChangeTargets(scope, batch.changes());
        SyncAccumulator acc = applyChanges(scope, epoch, adapter, batch.changes());
        validateSyncedSample(scope, epoch, adapter, batch.changes());
        return finishSync(scope, epoch, checkpoint, acc.errors == 0 ? batch.completeThrough() : Optional.empty(),
                "INCREMENTAL", start, acc);
    }

    /** Full enumeration has no certified change-log boundary and therefore never advances its cursor. */
    public SyncResult fullSync(CatalogDescriptor deployment, FederatedCatalog adapter) {
        Instant start = Instant.now();
        CatalogDeploymentIdentity scope = scope(deployment);
        checkCancellation();
        long epoch = beginObservation(scope);
        Optional<Instant> checkpoint = synchronization.loadCheckpoint(scope);
        List<String> schemas = adapter.listSchemas();
        applyProjection(epoch, new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), scope, schemas));
        SyncAccumulator acc = syncAllTables(scope, epoch, adapter, schemas);
        return finishSync(scope, epoch, checkpoint, Optional.empty(), "FULL", start, acc);
    }

    long beginObservation(CatalogDeploymentIdentity scope) {
        checkCancellation();
        projections.recoverPending(scope);
        synchronization.recoverPending(scope);
        return projections.beginObservation(scope);
    }

    private static CatalogDeploymentIdentity scope(CatalogDescriptor deployment) {
        return deployment.identity();
    }

    /**
     * Attempts incremental sync with retries, falling back to full sync if all incremental
     * attempts fail. Partial results are failures for this retrying entry point. An unconfirmed
     * persistence outcome or a stale deployment/checkpoint is terminal and never triggers full sync.
     * Full enumeration does not certify a change-log boundary or detect omitted deletions.
     */
    public SyncResult syncWithRetry(CatalogDescriptor deployment, FederatedCatalog adapter) {
        String catalogName = deployment.name();
        Exception lastError = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return requireComplete(incrementalSync(deployment, adapter));
            } catch (MetadataSyncOutcomeUnknownException | MetadataSyncConflictException | MetadataSyncPendingException
                    | MetadataProjectionConflictException | MetadataProjectionOutcomeUnknownException
                    | MetadataProjectionScopeException | MetadataProjectionPendingException | CancellationException terminal) {
                throw terminal;
            } catch (Exception e) {
                lastError = e;
                handleRetryFailure(catalogName, attempt, e);
            }
        }
        LOG.warn("event=incremental_sync_exhausted catalog={} attempts={} action=fallback_to_full",
                catalogName, MAX_RETRIES);
        try {
            return requireComplete(fullSync(deployment, adapter));
        } catch (MetadataSyncOutcomeUnknownException | MetadataSyncConflictException | MetadataSyncPendingException
                    | MetadataProjectionConflictException | MetadataProjectionOutcomeUnknownException
                    | MetadataProjectionScopeException | MetadataProjectionPendingException | CancellationException terminal) {
            throw terminal;
        } catch (Exception fullSyncError) {
            if (lastError != fullSyncError) {
                fullSyncError.addSuppressed(lastError);
            }
            throw new MetadataSyncException(catalogName, MAX_RETRIES, fullSyncError);
        }
    }

    private static void checkCancellation() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Metadata synchronization interrupted");
        }
    }

    private void handleRetryFailure(String catalogName, int attempt, Exception e) {
        checkCancellation();
        if (attempt < MAX_RETRIES) {
            long backoffMs = 1000L * (1L << (attempt - 1));
            ThreadUtils.sleepQuietly(backoffMs);
            checkCancellation();
        }
        LOG.warn("event=metadata_sync_failed catalog={} attempt={} maxRetries={} error={}",
                catalogName, attempt, MAX_RETRIES, e.getMessage(), e);
    }

    private SyncAccumulator applyChanges(CatalogDeploymentIdentity scope, long epoch, FederatedCatalog adapter, List<MetadataChange> changes) {
        SyncAccumulator acc = new SyncAccumulator();
        for (MetadataChange change : changes) {
            checkCancellation();
            try {
                applyChange(scope, epoch, adapter, change);
                acc.applied++;
            } catch (MetadataProjectionConflictException | MetadataProjectionOutcomeUnknownException
                    | MetadataProjectionScopeException | MetadataProjectionPendingException | CancellationException terminal) {
                throw terminal;
            } catch (Exception e) {
                acc.errors++;
                acc.errorMessages.add(e.getMessage() == null ? e.toString() : e.getMessage());
            }
        }
        return acc;
    }

    /**
     * Validates a random 10% sample of synced tables against the actual catalog to detect
     * schema drift. On drift: forces re-sync of the drifted table, writes an invalidation
     * entry, and increments the {@code metadata.schema_drift_detected} counter.
     *
     * <p>Only non-DELETE changes are sampled since deleted tables have no catalog-side
     * metadata to compare against.</p>
     */
    void validateSyncedSample(CatalogDeploymentIdentity scope, long epoch, FederatedCatalog adapter,
                              List<MetadataChange> changes) {
        for (MetadataChange change : changes) {
            checkCancellation();
            if (change.type() == MetadataChangeType.DELETE) {
                continue;
            }
            if (ThreadLocalRandom.current().nextDouble() >= DRIFT_SAMPLE_RATE) {
                continue;
            }
            validateSingleTable(scope, epoch, adapter, change);
        }
    }

    // The sampled path and deterministic component checks use this same actual drift detector.
    void validateSingleTable(CatalogDeploymentIdentity scope, long epoch, FederatedCatalog adapter,
                             MetadataChange change) {
        String catalogName = scope.catalogName();
        validateChangeTargets(scope, List.of(change));
        QualifiedTableRef ref = change.tableRef();
        try {
            TableMetadata actual = adapter.observeTable(ref.schema(), ref.table()).orElseThrow();
            validateTableTarget(scope, ref, actual);
            TableMetadata stored = tables.loadTable(catalogName, ref.schema(), ref.table())
                    .orElse(null);
            if (stored == null || !columnsMatch(actual, stored)) {
                LOG.error("event=schema_drift_detected catalog={} table={}.{}",
                        catalogName, ref.schema(), ref.table());
                applyProjection(epoch, new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), scope, actual));
                cache.invalidateTable(catalogName, ref.schema(), ref.table());
                schemaDriftDetectedCount.incrementAndGet();
            }
        } catch (MetadataProjectionConflictException | MetadataProjectionOutcomeUnknownException
                | MetadataProjectionScopeException | MetadataProjectionPendingException | CancellationException terminal) {
            throw terminal;
        } catch (Exception e) {
            LOG.warn("event=drift_validation_failed catalog={} table={}.{} error={}",
                    catalogName, ref.schema(), ref.table(), e.getMessage());
        }
    }

    private boolean columnsMatch(TableMetadata actual, TableMetadata stored) {
        return actual.columns().equals(stored.columns());
    }

    /**
     * Returns the number of schema drift events detected since this service was created.
     */
    public long getSchemaDriftDetectedCount() {
        return schemaDriftDetectedCount.get();
    }

    private SyncAccumulator syncAllTables(CatalogDeploymentIdentity scope, long epoch,
            FederatedCatalog adapter, List<String> schemas) {
        SyncAccumulator acc = new SyncAccumulator();
        for (String schema : schemas) {
            checkCancellation();
            syncSchema(scope, epoch, adapter, schema, acc);
        }
        return acc;
    }

    /**
     * Subscribes to push-based change notifications from the given catalog adapter.
     * When a change is received, the affected table is immediately synced and an
     * invalidation entry is written so other coordinators pick up the change without
     * waiting for the next poll interval.
     *
     * <p>This should be called during service initialization for each connector that
     * supports push-based notifications (i.e., has a non-default
     * {@link FederatedCatalog#subscribeToChanges} implementation).</p>
     */
    public void subscribeToPushNotifications(CatalogDescriptor deployment, FederatedCatalog adapter) {
        CatalogDeploymentIdentity scope = deployment.identity();
        adapter.subscribeToChanges(change -> {
            validateChangeTargets(scope, List.of(change));
            QualifiedTableRef ref = change.tableRef();
            LOG.info("event=push_change_received catalog={} table={}.{} type={}",
                    scope.catalogName(), ref.schema(), ref.table(), change.type());
            long epoch = beginObservation(scope);
            applyChange(scope, epoch, adapter, change);
        });
    }

    private void syncSchema(CatalogDeploymentIdentity scope, long epoch, FederatedCatalog adapter,
            String schema, SyncAccumulator acc) {
        for (String table : adapter.listTables(schema)) {
            checkCancellation();
            syncSingleTable(scope, epoch, adapter, schema, table, acc);
        }
    }

    private void syncSingleTable(CatalogDeploymentIdentity scope, long epoch, FederatedCatalog adapter, String schema, String table,
            SyncAccumulator acc) {
        try {
            TableMetadata metadata = adapter.observeTable(schema, table).orElseThrow();
            validateTableTarget(scope, new QualifiedTableRef(scope.catalogName(), schema, table), metadata);
            applyProjection(epoch, new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), scope, metadata));
            cache.invalidateTable(scope.catalogName(), schema, table);
            acc.applied++;
        } catch (MetadataProjectionConflictException | MetadataProjectionOutcomeUnknownException
                | MetadataProjectionScopeException | MetadataProjectionPendingException | CancellationException terminal) {
            throw terminal;
        } catch (Exception e) {
            acc.errors++;
            acc.errorMessages.add(e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    private void applyChange(CatalogDeploymentIdentity scope, long epoch, FederatedCatalog adapter, MetadataChange hint) {
        QualifiedTableRef ref = hint.tableRef();
        Optional<TableMetadata> observed = adapter.observeTable(ref.schema(), ref.table());
        observed.ifPresent(table -> validateTableTarget(scope, ref, table));
        MetadataProjectionMutation mutation = observed.isPresent()
                ? new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), scope, observed.orElseThrow())
                : new MetadataProjectionMutation.DeleteTable(UUID.randomUUID(), scope, ref);
        MetadataProjectionReceipt receipt = applyProjection(epoch, mutation);
        MetadataChangeType effect = MetadataChangeType.DELETE;
        if (observed.isPresent()) {
            effect = receipt.previousTable().isPresent() ? MetadataChangeType.UPDATE : MetadataChangeType.ADD;
        }
        MetadataChange change = new MetadataChange(effect, ref, observed.orElse(null));
        if (observed.isPresent() && hint.metadata() != null
                && !columnsMatch(observed.orElseThrow(), hint.metadata())) {
            schemaDriftDetectedCount.incrementAndGet();
        }
        cache.invalidateTable(ref.catalog(), ref.schema(), ref.table());
        eventDispatcher.dispatch(new MetadataEvent.MetadataSyncedEvent(scope.catalogName(), change));
        if (change.type() == MetadataChangeType.UPDATE && receipt.previousTable().isPresent()) {
            dispatchSchemaEvolutionIfChanged(scope.catalogName(), ref, receipt.previousTable().orElseThrow(),
                    change.metadata(), receipt.allowedTenants());
        }
    }

    private MetadataProjectionReceipt applyProjection(long epoch, MetadataProjectionMutation mutation) {
        checkCancellation();
        try {
            return projections.apply(epoch, mutation);
        } catch (MetadataProjectionOutcomeUnknownException unknown) {
            try {
                Optional<MetadataProjectionReceipt> receipt = projections.findReceipt(mutation.operationId());
                if (receipt.isPresent() && receipt.orElseThrow().mutation().equals(mutation)) {
                    return receipt.orElseThrow();
                }
            } catch (RuntimeException confirmationFailure) {
                if (confirmationFailure != unknown) unknown.addSuppressed(confirmationFailure);
            }
            throw unknown;
        }
    }

    private static void validateChangeTargets(CatalogDeploymentIdentity scope, List<MetadataChange> changes) {
        for (MetadataChange change : changes) {
            checkCancellation();
            QualifiedTableRef ref = java.util.Objects.requireNonNull(change.tableRef(), "source table reference");
            if (!scope.catalogName().equals(ref.catalog())) {
                throw new MetadataProjectionScopeException(
                        "Source change is outside its originating catalog deployment");
            }
            if (change.metadata() != null) validateTableTarget(scope, ref, change.metadata());
        }
    }

    private static void validateTableTarget(CatalogDeploymentIdentity scope, QualifiedTableRef expected,
            TableMetadata actual) {
        java.util.Objects.requireNonNull(actual, "source table metadata");
        if (!scope.catalogName().equals(actual.catalog()) || !expected.catalog().equals(actual.catalog())
                || !expected.schema().equals(actual.schema()) || !expected.table().equals(actual.table())) {
            throw new MetadataProjectionScopeException(
                    "Source metadata does not match the requested projection target");
        }
    }

    /**
     * Detects column-level schema changes between old and new metadata, validates them
     * via {@link SchemaEvolutionValidator}, and dispatches a {@link MetadataEvent.SchemaEvolutionEvent}
     * so that dependent systems (indexes, dynamic tables) can react.
     */
    private void dispatchSchemaEvolutionIfChanged(String catalogName, QualifiedTableRef ref,
            TableMetadata oldMetadata, TableMetadata newMetadata, java.util.Set<String> allowedTenants) {
        if (oldMetadata.columns().equals(newMetadata.columns())) {
            return; // No schema change
        }
        List<SchemaChange> schemaChanges = detectSchemaChanges(oldMetadata, newMetadata);
        if (schemaChanges.isEmpty()) {
            return;
        }
        SchemaValidationResult result = schemaEvolutionValidator.validate(oldMetadata, schemaChanges);
        if (!result.compatible()) {
            LOG.error("event=incompatible_schema_evolution catalog={} table={}.{} errors={}",
                    catalogName, ref.schema(), ref.table(), result.errors());
        }
        LOG.info("event=schema_evolution_detected catalog={} table={}.{} changes={}",
                catalogName, ref.schema(), ref.table(), schemaChanges.size());
        eventDispatcher.dispatch(new MetadataEvent.SchemaEvolutionEvent(
                catalogName, ref, schemaChanges, allowedTenants.isEmpty()
                        ? MetadataEvent.TenantImpactScope.global()
                        : MetadataEvent.TenantImpactScope.tenants(allowedTenants)));
    }

    /**
     * Computes the list of schema changes between old and new table metadata
     * by comparing column lists.
     */
    private static List<SchemaChange> detectSchemaChanges(TableMetadata oldMeta, TableMetadata newMeta) {
        List<SchemaChange> changes = new ArrayList<>();
        var oldColumns = oldMeta.columns();
        var newColumns = newMeta.columns();

        detectAddedColumns(oldColumns, newColumns, changes);
        detectDroppedColumns(oldColumns, newColumns, changes);
        detectColumnTypeChanges(oldColumns, newColumns, changes);

        return changes;
    }

    /** Detects columns present in the new metadata but absent from the old one. */
    private static void detectAddedColumns(
            List<org.pangrova.metadata.api.object.ColumnMetadata> oldColumns,
            List<org.pangrova.metadata.api.object.ColumnMetadata> newColumns,
            List<SchemaChange> changes) {
        var oldNames = new java.util.HashSet<String>();
        for (var col : oldColumns) {
            oldNames.add(col.name());
        }
        for (var col : newColumns) {
            if (!oldNames.contains(col.name())) {
                changes.add(new SchemaChange.AddColumn(col.name(), col.type(), col.nullable()));
            }
        }
    }

    /** Detects columns present in the old metadata but absent from the new one. */
    private static void detectDroppedColumns(
            List<org.pangrova.metadata.api.object.ColumnMetadata> oldColumns,
            List<org.pangrova.metadata.api.object.ColumnMetadata> newColumns,
            List<SchemaChange> changes) {
        var newNames = new java.util.HashSet<String>();
        for (var col : newColumns) {
            newNames.add(col.name());
        }
        for (var col : oldColumns) {
            if (!newNames.contains(col.name())) {
                changes.add(new SchemaChange.DropColumn(col.name()));
            }
        }
    }

    /** Detects columns whose declared type changed between old and new metadata. */
    private static void detectColumnTypeChanges(
            List<org.pangrova.metadata.api.object.ColumnMetadata> oldColumns,
            List<org.pangrova.metadata.api.object.ColumnMetadata> newColumns,
            List<SchemaChange> changes) {
        var newByName = new java.util.HashMap<String, org.pangrova.metadata.api.object.ColumnMetadata>();
        for (var col : newColumns) {
            newByName.put(col.name(), col);
        }
        for (var oldCol : oldColumns) {
            var newCol = newByName.get(oldCol.name());
            if (newCol != null && !oldCol.type().equals(newCol.type())) {
                changes.add(new SchemaChange.ChangeType(oldCol.name(), newCol.type()));
            }
        }
    }

    private static SyncResult requireComplete(SyncResult result) {
        if (result.errors() != 0) {
            throw new IllegalStateException("Metadata synchronization for catalog=" + result.catalogName()
                    + " has " + result.errors() + " failed changes");
        }
        return result;
    }

    private SyncResult finishSync(CatalogDeploymentIdentity scope, long epoch, Optional<Instant> expected, Optional<Instant> next,
            String syncType, Instant start, SyncAccumulator acc) {
        checkCancellation();
        Instant end = Instant.now();
        SyncResult result = new SyncResult(scope.catalogName(), syncType, end, acc.applied, acc.errors,
                acc.errorMessages, Math.max(0, Duration.between(start, end).toMillis()));
        var completion = new MetadataSyncCompletion(UUID.randomUUID(), scope, expected, next, result);
        try {
            synchronization.complete(epoch, completion);
        } catch (MetadataSyncOutcomeUnknownException unknown) {
            // Never launch another run to resolve an unconfirmed COMMIT.
            try {
                if (synchronization.findCompletion(completion.runId()).filter(completion::equals).isPresent()) {
                    return result;
                }
            } catch (RuntimeException confirmationFailure) {
                if (confirmationFailure != unknown) unknown.addSuppressed(confirmationFailure);
            }
            throw unknown;
        }
        return result;
    }

    /**
     * Mutable accumulator for sync progress tracking within a single sync operation.
     */
    private static final class SyncAccumulator {
        int applied;
        int errors;
        final List<String> errorMessages = new ArrayList<>();
    }
}
