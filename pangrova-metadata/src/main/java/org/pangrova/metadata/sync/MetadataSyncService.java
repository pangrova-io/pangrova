package org.pangrova.metadata.sync;

import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.cache.MetadataCache;
import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.exception.MetadataSyncException;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.schema.SchemaValidationResult;
import org.pangrova.metadata.store.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

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

    private final MetadataStore store;
    private final MetadataCache cache;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionValidator schemaEvolutionValidator;
    private final AtomicLong schemaDriftDetectedCount = new AtomicLong();

    /** Shares the persistent mutation authority, local cache and metadata event dispatcher. */
    public MetadataSyncService(MetadataStore store, MetadataCache cache, MetadataEventDispatcher eventDispatcher) {
        this.store = store;
        this.cache = cache;
        this.eventDispatcher = eventDispatcher;
        this.schemaEvolutionValidator = new SchemaEvolutionValidator();
    }

    /** Applies only changes recorded since the last sync of the given catalog. */
    public SyncResult incrementalSync(String catalogName, FederatedCatalog adapter) {
        Instant start = Instant.now();
        Instant lastSync = store.getLastSyncTime(catalogName).orElse(Instant.EPOCH);
        List<MetadataChange> changes = adapter.getChangesSince(lastSync);
        SyncAccumulator acc = applyChanges(catalogName, changes);
        validateSyncedSample(catalogName, adapter, changes);
        return finishSync(catalogName, "INCREMENTAL", start, acc);
    }

    /** Reloads the full schema and table inventory of the given catalog. */
    public SyncResult fullSync(String catalogName, FederatedCatalog adapter) {
        Instant start = Instant.now();
        List<String> schemas = adapter.listSchemas();
        store.saveSchemas(catalogName, schemas);
        SyncAccumulator acc = syncAllTables(catalogName, adapter, schemas);
        return finishSync(catalogName, "FULL", start, acc);
    }

    /**
     * Attempts incremental sync with retries, falling back to full sync if all incremental
     * attempts fail. This ensures sync reliability even when incremental change detection
     * is broken or returns incomplete results.
     */
    public SyncResult syncWithRetry(String catalogName, FederatedCatalog adapter) {
        Exception lastError = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return incrementalSync(catalogName, adapter);
            } catch (Exception e) {
                lastError = e;
                handleRetryFailure(catalogName, attempt, e);
            }
        }
        LOG.warn("event=incremental_sync_exhausted catalog={} attempts={} action=fallback_to_full",
                catalogName, MAX_RETRIES);
        try {
            return fullSync(catalogName, adapter);
        } catch (Exception fullSyncError) {
            fullSyncError.addSuppressed(lastError);
            throw new MetadataSyncException(catalogName, MAX_RETRIES, fullSyncError);
        }
    }

    private void handleRetryFailure(String catalogName, int attempt, Exception e) {
        if (attempt < MAX_RETRIES) {
            long backoffMs = 1000L * (1L << (attempt - 1));
            ThreadUtils.sleepQuietly(backoffMs);
            if (Thread.currentThread().isInterrupted()) {
                throw new MetadataSyncException("Sync interrupted during backoff", 0, e);
            }
        }
        LOG.warn("event=metadata_sync_failed catalog={} attempt={} maxRetries={} error={}",
                catalogName, attempt, MAX_RETRIES, e.getMessage(), e);
    }

    private SyncAccumulator applyChanges(String catalogName, List<MetadataChange> changes) {
        SyncAccumulator acc = new SyncAccumulator();
        for (MetadataChange change : changes) {
            try {
                applyChange(catalogName, change);
                acc.applied++;
            } catch (Exception e) {
                acc.errors++;
                acc.errorMessages.add(e.getMessage());
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
    void validateSyncedSample(String catalogName, FederatedCatalog adapter,
                              List<MetadataChange> changes) {
        for (MetadataChange change : changes) {
            if (change.type() == MetadataChange.ChangeType.DELETE) {
                continue;
            }
            if (ThreadLocalRandom.current().nextDouble() >= DRIFT_SAMPLE_RATE) {
                continue;
            }
            validateSingleTable(catalogName, adapter, change);
        }
    }

    // The sampled path and deterministic component checks use this same actual drift detector.
    void validateSingleTable(String catalogName, FederatedCatalog adapter,
                             MetadataChange change) {
        QualifiedTableRef ref = change.tableRef();
        try {
            TableMetadata actual = adapter.getTable(ref.schema(), ref.table());
            TableMetadata stored = store.loadTable(catalogName, ref.schema(), ref.table())
                    .orElse(null);
            if (stored == null || !columnsMatch(actual, stored)) {
                LOG.error("event=schema_drift_detected catalog={} table={}.{}",
                        catalogName, ref.schema(), ref.table());
                store.saveTable(actual);
                cache.invalidateTable(catalogName, ref.schema(), ref.table());
                schemaDriftDetectedCount.incrementAndGet();
            }
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

    private SyncAccumulator syncAllTables(String catalogName, FederatedCatalog adapter, List<String> schemas) {
        SyncAccumulator acc = new SyncAccumulator();
        for (String schema : schemas) {
            syncSchema(catalogName, adapter, schema, acc);
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
    public void subscribeToPushNotifications(String catalogName, FederatedCatalog adapter) {
        adapter.subscribeToChanges(change -> {
            SyncAccumulator acc = new SyncAccumulator();
            QualifiedTableRef ref = change.tableRef();
            LOG.info("event=push_change_received catalog={} table={}.{} type={}",
                    catalogName, ref.schema(), ref.table(), change.type());
            syncSingleTable(catalogName, adapter, ref.schema(), ref.table(), acc);
        });
    }

    private void syncSchema(String catalogName, FederatedCatalog adapter, String schema, SyncAccumulator acc) {
        for (String table : adapter.listTables(schema)) {
            syncSingleTable(catalogName, adapter, schema, table, acc);
        }
    }

    private void syncSingleTable(String catalogName, FederatedCatalog adapter, String schema, String table,
            SyncAccumulator acc) {
        try {
            TableMetadata metadata = adapter.getTable(schema, table);
            store.saveTable(metadata);
            cache.invalidateTable(catalogName, schema, table);
            acc.applied++;
        } catch (Exception e) {
            acc.errors++;
            acc.errorMessages.add(e.getMessage());
        }
    }

    private void applyChange(String catalogName, MetadataChange change) {
        QualifiedTableRef ref = change.tableRef();

        // Load existing metadata before applying the change (for schema evolution detection)
        Optional<TableMetadata> existingMetadata = Optional.empty();
        if (change.type() == MetadataChange.ChangeType.UPDATE) {
            existingMetadata = store.loadTable(ref.catalog(), ref.schema(), ref.table());
        }

        if (change.type() == MetadataChange.ChangeType.DELETE) {
            store.deleteTable(ref.catalog(), ref.schema(), ref.table());
        } else {
            store.saveTable(change.metadata());
        }
        cache.invalidateTable(ref.catalog(), ref.schema(), ref.table());
        eventDispatcher.dispatch(new MetadataEvent.MetadataSyncedEvent(catalogName, change));

        // Schema evolution detection: when UPDATE changes column structure, validate and dispatch
        if (change.type() == MetadataChange.ChangeType.UPDATE && existingMetadata.isPresent()) {
            dispatchSchemaEvolutionIfChanged(catalogName, ref, existingMetadata.get(), change.metadata());
        }
    }

    /**
     * Detects column-level schema changes between old and new metadata, validates them
     * via {@link SchemaEvolutionValidator}, and dispatches a {@link MetadataEvent.SchemaEvolutionEvent}
     * so that dependent systems (indexes, dynamic tables) can react.
     */
    private void dispatchSchemaEvolutionIfChanged(String catalogName, QualifiedTableRef ref,
                                                   TableMetadata oldMetadata, TableMetadata newMetadata) {
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
                catalogName, ref, schemaChanges, resolveTenantScope(catalogName)));
    }

    private MetadataEvent.TenantImpactScope resolveTenantScope(String catalogName) {
        var descriptor = store.getCatalogDescriptor(catalogName)
                .orElseThrow(() -> new IllegalStateException(
                        "Catalog descriptor is required for schema evolution: " + catalogName));
        return descriptor.allowedTenants().isEmpty()
                ? MetadataEvent.TenantImpactScope.global()
                : MetadataEvent.TenantImpactScope.tenants(descriptor.allowedTenants());
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
            List<org.pangrova.metadata.model.ColumnMetadata> oldColumns,
            List<org.pangrova.metadata.model.ColumnMetadata> newColumns,
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
            List<org.pangrova.metadata.model.ColumnMetadata> oldColumns,
            List<org.pangrova.metadata.model.ColumnMetadata> newColumns,
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
            List<org.pangrova.metadata.model.ColumnMetadata> oldColumns,
            List<org.pangrova.metadata.model.ColumnMetadata> newColumns,
            List<SchemaChange> changes) {
        var newByName = new java.util.HashMap<String, org.pangrova.metadata.model.ColumnMetadata>();
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

    private SyncResult finishSync(String catalogName, String syncType, Instant start, SyncAccumulator acc) {
        Instant end = Instant.now();
        if (acc.applied > 0) {
            store.updateSyncTime(catalogName, end);
        }
        SyncResult result = new SyncResult(
                catalogName, syncType, end, acc.applied, acc.errors,
                acc.errorMessages, Duration.between(start, end).toMillis());
        store.recordSyncHistory(result);
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
