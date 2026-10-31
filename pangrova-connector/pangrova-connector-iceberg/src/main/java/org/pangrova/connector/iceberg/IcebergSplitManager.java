package org.pangrova.connector.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableSplit;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableSplit;
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergFileTableSplit;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.FixedSplitSource;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ColumnMapping;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Iceberg split manager that plans file scan tasks from table snapshots.
 * Delegates to real Iceberg TableScan API for split enumeration.
 */
public class IcebergSplitManager implements ConnectorSplitManager {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSplitManager.class);
    private static final Duration DEFAULT_MANIFEST_CACHE_TTL = Duration.ofMinutes(5);
    private static final int DEFAULT_MANIFEST_CACHE_MAX_SIZE = 1000;

    private final String catalogName;
    private final Catalog catalog;
    private final QuerySnapshotRegistry snapshotRegistry;
    private final IcebergTaskToSplit taskToSplit;

    /**
     * Caches planned splits keyed by (tableName + snapshotId) to avoid
     * re-reading manifest files on repeated planSplits calls for the same
     * snapshot. Invalidated on commit via {@link #invalidateManifestCache(String)}.
     */
    private final Cache<String, List<ConnectorSplit>> manifestCache;

    public IcebergSplitManager(String catalogName, Catalog catalog,
                              IcebergExpressionToSubstrait expressionConverter) {
        this(catalogName, catalog, expressionConverter, (QuerySnapshotRegistry) null);
    }

    public IcebergSplitManager(String catalogName, Catalog catalog,
                              IcebergExpressionToSubstrait expressionConverter,
                              IcebergMetadata metadata) {
        this(catalogName, catalog, expressionConverter, (QuerySnapshotRegistry) metadata);
    }

    /** Creates a split manager with an explicit query snapshot registry. */
    public IcebergSplitManager(String catalogName, Catalog catalog,
                              IcebergExpressionToSubstrait expressionConverter,
                              QuerySnapshotRegistry snapshotRegistry) {
        this.catalogName = catalogName;
        this.catalog = catalog;
        this.snapshotRegistry = snapshotRegistry;
        this.taskToSplit = new IcebergTaskToSplit(expressionConverter);
        this.manifestCache = Caffeine.newBuilder()
                .expireAfterWrite(DEFAULT_MANIFEST_CACHE_TTL)
                .maximumSize(DEFAULT_MANIFEST_CACHE_MAX_SIZE)
                .build();
    }

    /**
     * Invalidates all cached manifest entries for the given table.
     * Called after a successful commit to ensure subsequent planSplits
     * reads fresh manifest data reflecting the new snapshot.
     */
    public void invalidateManifestCache(String tableName) {
        manifestCache.asMap().keySet().removeIf(key -> key.startsWith(tableName + "|")
                || key.contains("." + tableName + "|"));
        LOG.debug("event=manifest_cache_invalidated table={}", tableName);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        if (table instanceof IcebergFileTableHandle files) {
            IcebergFileReadBindings.validate(files, resolveTable(files.baseTable()));
            return new FixedSplitSource(files.manifests().stream()
                    .map(manifest -> (ConnectorSplit) new IcebergFileTableSplit(files, manifest))
                    .toList());
        }
        if (table instanceof IcebergManifestTableHandle manifests) {
            IcebergManifestReadBindings.validate(manifests, resolveTable(manifests.baseTable()));
            return new FixedSplitSource(manifests.empty() ? List.of()
                    : List.of(new IcebergManifestTableSplit(manifests)));
        }
        if (table instanceof IcebergSystemTableHandle system) {
            return new FixedSplitSource(List.of(new IcebergSystemTableSplit(
                    system.tableName(), system.kind(), system.metadataLocation(),
                    system.metadataLength())));
        }
        IcebergTableHandle icebergTable = (IcebergTableHandle) table;
        Table icebergSdkTable = resolveTable(icebergTable);
        IcebergReadBindings.validateSelected(icebergSdkTable, icebergTable);
        List<ConnectorSplit> splits = planSplitsByReadMode(icebergSdkTable, icebergTable, constraint, session);
        LOG.info("event=splits_planned count={} table={}.{} catalog={} read_mode={} snapshot={}",
                splits.size(), icebergTable.schemaName(), icebergTable.tableName(),
                catalogName, icebergTable.readMode(),
                icebergSdkTable.currentSnapshot() != null ? icebergSdkTable.currentSnapshot().snapshotId() : "null");
        return new FixedSplitSource(splits);
    }

    /**
     * Returns only the data files appended between two snapshots using
     * Iceberg's {@link IncrementalAppendScan} API.
     *
     * <p>This enables O(delta) refresh for Dynamic Tables instead of
     * scanning the entire table on every refresh cycle.</p>
     */
    @Override
    public ConnectorSplitSource getIncrementalSplits(
            ConnectorSession session,
            ConnectorTableHandle table,
            long fromSnapshotId,
            long toSnapshotId,
            Constraint constraint) {
        IcebergTableHandle icebergTable = (IcebergTableHandle) table;
        Table icebergSdkTable = resolveTable(icebergTable);
        validateSnapshotRange(fromSnapshotId, toSnapshotId);
        String tableName = sourceId(icebergTable);
        LOG.debug("event=incremental_splits_start table={}.{} from_snapshot={} to_snapshot={}",
                icebergTable.schemaName(), tableName, fromSnapshotId, toSnapshotId);
        List<ConnectorSplit> splits = scanIncrementalFiles(
                icebergSdkTable, tableName, fromSnapshotId, toSnapshotId);
        LOG.debug("event=incremental_splits_complete table={}.{} split_count={}",
                icebergTable.schemaName(), tableName, splits.size());
        return new FixedSplitSource(splits);
    }

    /**
     * Loads a fresh table from the catalog to pick up the latest snapshot.
     * Iceberg's loadTable returns a snapshot-consistent view from the catalog;
     * no explicit refresh is needed (and calling refresh() on REST catalogs
     * triggers a redundant metadata fetch).
     */
    private Table resolveTable(IcebergTableHandle handle) {
        return catalog.loadTable(
                TableIdentifier.of(handle.schemaName(), handle.tableName()));
    }

    private List<ConnectorSplit> planSplitsByReadMode(
            Table table, IcebergTableHandle handle, Constraint constraint,
            ConnectorSession session) {
        return switch (handle.readMode()) {
            case IcebergTableHandle.READ_MODE_INCREMENTAL ->
                    planIncrementalSplits(table, handle, constraint);
            case IcebergTableHandle.READ_MODE_TIME_TRAVEL ->
                    planTimeTravelSplits(table, handle, constraint);
            default -> planFullSplits(table, handle, constraint, session);
        };
    }

    /**
     * Plans splits for a full snapshot scan (default mode).
     * Uses the pinned snapshot (if available for this query), the snapshot
     * specified in the handle, or the current snapshot. When a snapshot is
     * resolved and a queryId is present in the session, the snapshot is
     * pinned via {@link IcebergMetadata#pinSnapshotForQuery} to prevent
     * concurrent compaction from removing the underlying data files
     * mid-query.
     *
     * <p>Unconstrained results are cached by (tableName|snapshotId) to avoid
     * re-reading manifests on repeated planSplits calls.</p>
     */
    private List<ConnectorSplit> planFullSplits(
            Table table, IcebergTableHandle handle, Constraint constraint,
            ConnectorSession session) {
        Snapshot snapshot = resolveSnapshotWithPin(table, handle, session);
        if (snapshot == null) {
            return List.of();
        }
        long snapshotId = snapshot.snapshotId();
        boolean cacheable = constraint == null || constraint == Constraint.EMPTY
                || constraint.getDomains().isEmpty();
        Schema readSchema = IcebergReadSchema.resolve(handle, table).schema();
        String cacheKey = sourceId(handle) + "|" + snapshotId + "|"
                + org.pangrova.common.util.DigestUtils.sha256Hex(
                        handle.properties().getOrDefault(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY, "")
                                + org.apache.iceberg.SchemaParser.toJson(readSchema));

        List<ConnectorSplit> cached = lookupCachedSplits(cacheable, cacheKey, handle, snapshotId);
        if (cached != null) {
            return cached;
        }

        TableScan scan = table.newScan().useSnapshot(snapshotId).project(readSchema);
        scan = applyConstraintFilter(scan, constraint);
        String tableName = sourceId(handle);
        List<ColumnMapping> columnMappings = IcebergTaskToSplit.computeColumnMappings(table, readSchema);
        List<ConnectorSplit> splits = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                splits.add(taskToSplit.convert(task, tableName, "FULL", snapshotId,
                        0L, 0L, readSchema, columnMappings));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to plan splits for table: " + table.name(), e);
        }

        List<ConnectorSplit> immutableSplits = List.copyOf(splits);
        if (cacheable) {
            manifestCache.put(cacheKey, immutableSplits);
            LOG.debug("event=manifest_cache_populated table={}.{} snapshot={} splits={}",
                    handle.schemaName(), handle.tableName(), snapshotId, immutableSplits.size());
        }
        return immutableSplits;
    }

    /** Returns the cached split list for an unconstrained scan, or null on miss. */
    private List<ConnectorSplit> lookupCachedSplits(boolean cacheable, String cacheKey,
                                                    IcebergTableHandle handle, long snapshotId) {
        if (!cacheable) {
            return null;
        }
        List<ConnectorSplit> cached = manifestCache.getIfPresent(cacheKey);
        if (cached != null) {
            LOG.debug("event=manifest_cache_hit table={}.{} snapshot={} splits={}",
                    handle.schemaName(), handle.tableName(), snapshotId, cached.size());
        }
        return cached;
    }

    /**
     * Resolves the snapshot to scan, respecting query-level snapshot pinning.
     *
     * <p>Resolution order:</p>
     * <ol>
     *   <li>If the handle specifies a snapshotId, use that (explicit snapshot selection).</li>
     *   <li>If a queryId is present in the session and the metadata has a pinned
     *       snapshot for that query, use the pinned snapshot to ensure all splits
     *       within a query see a consistent view even under concurrent compaction.</li>
     *   <li>Otherwise, use the table's current snapshot and pin it for the query.</li>
     * </ol>
     *
     * @param table   the resolved Iceberg table
     * @param handle  the table handle (may contain an explicit snapshotId)
     * @param session the connector session (may contain queryId in properties)
     * @return the resolved snapshot, or null if the table has no snapshots
     */
    private Snapshot resolveSnapshotWithPin(Table table, IcebergTableHandle handle,
                                            ConnectorSession session) {
        // Explicit snapshot on handle takes precedence (e.g. from optimizer)
        if (handle.snapshotId().isPresent()) {
            Snapshot explicit = IcebergReadBindings.selectedSnapshot(table, handle);
            pinIfQueryPresent(session, handle, explicit);
            return explicit;
        }

        String queryId = session != null ? session.getProperties().get("queryId") : null;

        // Check for an already-pinned snapshot from beginQuery / getTableStatistics
        if (queryId != null && snapshotRegistry != null) {
            java.util.Optional<Long> pinned = snapshotRegistry.getPinnedSnapshot(
                    queryId, handle.schemaName(), handle.tableName());
            if (pinned.isPresent()) {
                Snapshot pinnedSnapshot = table.snapshot(pinned.get());
                if (pinnedSnapshot != null) {
                    LOG.debug("event=using_pinned_snapshot queryId={} catalog={} snapshotId={}",
                            queryId, catalogName, pinned.get());
                    return pinnedSnapshot;
                }
                throw new IllegalStateException("Selected query snapshot expired: " + pinned.get());
            }
        }

        // Select once for direct SDK callers that do not carry an already-bound handle.
        Snapshot current = table.currentSnapshot();
        pinIfQueryPresent(session, handle, current);
        return current;
    }

    /**
     * Pins the given snapshot for the current query if both a queryId and
     * metadata are available. This ensures that subsequent split planning
     * calls within the same query use the same snapshot.
     */
    private void pinIfQueryPresent(ConnectorSession session, IcebergTableHandle handle, Snapshot snapshot) {
        if (snapshot == null || snapshotRegistry == null || session == null) {
            return;
        }
        String queryId = session.getProperties().get("queryId");
        if (queryId != null) {
            snapshotRegistry.pinSnapshotForQuery(
                    queryId, handle.schemaName(), handle.tableName(), snapshot.snapshotId());
        }
    }

    /**
     * Plans splits for incremental reads — only data files added between
     * fromSnapshotId (exclusive) and toSnapshotId (inclusive).
     *
     * <p>Uses Iceberg's IncrementalAppendScan API which returns only the
     * newly appended data files in the snapshot range.</p>
     *
     * <p>Validates: Requirements R7.1</p>
     */
    private List<ConnectorSplit> planIncrementalSplits(
            Table table, IcebergTableHandle handle, Constraint constraint) {
        long fromSnapshot = handle.fromSnapshotId();
        long toSnapshot = handle.toSnapshotId();
        validateSnapshotRange(fromSnapshot, toSnapshot);
        String tableName = sourceId(handle);
        LOG.debug("event=incremental_scan_start table={}.{} from_snapshot={} to_snapshot={}",
                handle.schemaName(), handle.tableName(), fromSnapshot, toSnapshot);

        List<ConnectorSplit> splits = scanIncrementalFiles(
                table, tableName, fromSnapshot, toSnapshot, constraint);
        LOG.debug("event=incremental_scan_complete table={}.{} split_count={}",
                handle.schemaName(), handle.tableName(), splits.size());
        return splits;
    }

    private static void validateSnapshotRange(long from, long to) {
        if (from <= 0 || to <= 0) {
            throw new IllegalArgumentException(
                    "Incremental read requires valid fromSnapshotId and toSnapshotId, got from="
                            + from + " to=" + to);
        }
    }

    private List<ConnectorSplit> scanIncrementalFiles(
            Table table, String qualifiedTableName, long fromSnapshot, long toSnapshot) {
        return scanIncrementalFiles(table, qualifiedTableName, fromSnapshot, toSnapshot, null);
    }

    private List<ConnectorSplit> scanIncrementalFiles(
            Table table, String qualifiedTableName, long fromSnapshot, long toSnapshot,
            Constraint constraint) {
        // Get fromSnapshot's sequence number for defensive filtering.
        // In format v2, each data file carries a dataSequenceNumber; files with
        // sequence <= fromSnapshot's sequence existed at or before fromSnapshot
        // and must be excluded from incremental results.
        Snapshot fromSnap = table.snapshot(fromSnapshot);
        long fromSequenceNumber = (fromSnap != null) ? fromSnap.sequenceNumber() : 0L;
        LOG.debug("event=incremental_scan_debug fromSnapshot={} fromSeqNum={} toSnapshot={} fromSnapNull={}",
                fromSnapshot, fromSequenceNumber, toSnapshot, fromSnap == null);

        IncrementalAppendScan scan = table.newIncrementalAppendScan()
                .fromSnapshotExclusive(fromSnapshot)
                .toSnapshot(toSnapshot);
        scan = applyIncrementalConstraintFilter(scan, constraint);
        List<ConnectorSplit> splits = new ArrayList<>();
        int skippedCount = 0;
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                Long fileSeqNum = task.file().dataSequenceNumber();
                LOG.debug("event=incremental_file file={} fileSeqNum={} fromSeqNum={}",
                        task.file().path(), fileSeqNum, fromSequenceNumber);
                // Defensive: skip files whose data sequence number indicates they
                // existed at or before fromSnapshot. This guards against edge cases
                // in IncrementalAppendScan (e.g., manifest merging or first-snapshot
                // boundary) where older files may be incorrectly included.
                if (fromSequenceNumber > 0) {
                    if (fileSeqNum != null && fileSeqNum <= fromSequenceNumber) {
                        skippedCount++;
                        continue;
                    }
                }
                splits.add(taskToSplit.convert(
                        task, qualifiedTableName, "INCREMENTAL", toSnapshot,
                        fromSnapshot, toSnapshot,
                        table.schema(), computeColumnMappings(table)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to plan incremental splits for table: " + table.name(), e);
        }
        LOG.debug("event=incremental_scan_result splits={} skipped={}", splits.size(), skippedCount);
        return splits;
    }

    /**
     * Applies constraint-based partition pruning to an IncrementalAppendScan.
     * Converts {@link Constraint} domains into Iceberg {@link Expression}s
     * and pushes them into the scan so manifest/partition pruning takes effect
     * on incremental scans, not just full-table scans.
     */
    private IncrementalAppendScan applyIncrementalConstraintFilter(
            IncrementalAppendScan scan, Constraint constraint) {
        try {
            Expression combined = IcebergConstraintExpressions.fromConstraint(constraint, scan.schema());
            if (combined == null) {
                return scan;
            }
            LOG.debug("event=iceberg_incremental_predicate_pushdown domainCount={}",
                    constraint.getDomains().size());
            return scan.filter(combined);
        } catch (Exception e) {
            LOG.warn("event=iceberg_incremental_predicate_pushdown_failed error={}", e.getMessage());
            return scan;
        }
    }

    /**
     * Plans splits for time-travel queries — reads at a specific snapshot.
     *
     * <p>Uses the snapshot ID from the handle to pin the scan to a historical
     * point in time. The native side reads these as normal data files.</p>
     *
     * <p>Validates: Requirements R7.3</p>
     */
    private List<ConnectorSplit> planTimeTravelSplits(
            Table table, IcebergTableHandle handle, Constraint constraint) {
        long snapshotId = handle.snapshotId()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Time-travel read requires a snapshotId on the table handle"));
        validateSnapshotExists(table, snapshotId);
        String tableName = sourceId(handle);
        LOG.debug("event=time_travel_scan_start table={}.{} snapshot_id={}",
                handle.schemaName(), handle.tableName(), snapshotId);

        List<ConnectorSplit> splits = scanTimeTravelFiles(
                table, handle, tableName, snapshotId, constraint);
        LOG.debug("event=time_travel_scan_complete table={}.{} split_count={}",
                handle.schemaName(), handle.tableName(), splits.size());
        return splits;
    }

    private static void validateSnapshotExists(Table table, long snapshotId) {
        if (table.snapshot(snapshotId) == null) {
            throw new IllegalArgumentException(
                    "Snapshot not found: " + snapshotId + " for table: " + table.name());
        }
    }

    private List<ConnectorSplit> scanTimeTravelFiles(
            Table table, IcebergTableHandle handle, String qualifiedTableName, long snapshotId, Constraint constraint) {
        Schema readSchema = IcebergReadSchema.resolve(handle, table).schema();
        TableScan scan = applyConstraintFilter(table.newScan().useSnapshot(snapshotId).project(readSchema), constraint);
        List<ColumnMapping> columnMappings = IcebergTaskToSplit.computeColumnMappings(table, readSchema);
        List<ConnectorSplit> splits = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                splits.add(taskToSplit.convert(task, qualifiedTableName, "TIME_TRAVEL", snapshotId,
                        0L, 0L,
                        readSchema, columnMappings));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to plan time-travel splits for table: " + table.name(), e);
        }
        return splits;
    }

    private TableScan applyConstraintFilter(TableScan scan, Constraint constraint) {
        try {
            Expression combined = IcebergConstraintExpressions.fromConstraint(constraint, scan.schema());
            if (combined == null) {
                return scan;
            }
            LOG.debug("event=iceberg_predicate_pushdown domainCount={}",
                    constraint.getDomains().size());
            return scan.filter(combined);
        } catch (Exception e) {
            LOG.warn("event=iceberg_predicate_pushdown_failed error={}", e.getMessage());
            return scan;
        }
    }

    private List<ColumnMapping> computeColumnMappings(Table table) {
        return IcebergTaskToSplit.computeColumnMappings(table, table.schema());
    }

    private String sourceId(IcebergTableHandle handle) {
        return catalogName + "." + handle.schemaName() + "." + handle.tableName();
    }

}
