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
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.runtime.scan.FixedSplitSource;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ColumnMapping;
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

import java.time.Duration;
import java.util.List;

/**
 * Iceberg split manager that plans file scan tasks from table snapshots.
 * Delegates to real Iceberg TableScan API for split enumeration.
 */
public class IcebergSplitManager implements ConnectorSplitManager {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSplitManager.class);
    private static final Duration DEFAULT_MANIFEST_CACHE_TTL = Duration.ofMinutes(5);
    // Limit estimated cache weight across completed scans and cap each scan separately.
    private static final long DEFAULT_MANIFEST_CACHE_MAX_WEIGHT = 64L * 1024 * 1024;
    // Conservative per-split heap estimate (path, partition values, delete files,
    // object overhead). Column mappings are shared per scan and excluded.
    private static final int ESTIMATED_BYTES_PER_SPLIT = 1024;
    private static final int MAX_CACHED_SPLITS_PER_SCAN = 4096;

    private final String catalogName;
    private final Catalog catalog;
    private final QuerySnapshotRegistry snapshotRegistry;
    private final IcebergTaskToSplit taskToSplit;

    /** Caches small completed scans by table, snapshot, source identity, selected schema and range target. */
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
                .maximumWeight(DEFAULT_MANIFEST_CACHE_MAX_WEIGHT)
                .weigher((String key, List<ConnectorSplit> splits) -> (int) Math.min(
                        Integer.MAX_VALUE,
                        Math.max(1L, (long) splits.size() * ESTIMATED_BYTES_PER_SPLIT)))
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
        return planBoundSplits(session, icebergTable, icebergSdkTable, constraint, null);
    }

    @Override
    public ConnectorSplitSource getBoundSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorMetadata metadata, ConnectorReadBinding binding,
            DynamicFilter dynamicFilter, Constraint constraint, SplitPlanningBudget budget) {
        budget.check(0);
        if (binding.state() == org.pangrova.connector.api.enums.ReadSnapshotState.EMPTY
                || !(binding.handle() instanceof IcebergTableHandle handle)) {
            return ConnectorSplitManager.super.getBoundSplits(
                    transaction, session, metadata, binding, dynamicFilter, constraint, budget);
        }
        Table table = IcebergCatalogTables.resolveTable(catalog, handle);
        IcebergReadBindings.validate(table, handle, binding);
        return planBoundSplits(session, handle, table, constraint, budget);
    }

    private ConnectorSplitSource planBoundSplits(ConnectorSession session, IcebergTableHandle handle,
            Table table, Constraint constraint, SplitPlanningBudget budget) {
        return switch (handle.readMode()) {
            case IcebergTableHandle.READ_MODE_INCREMENTAL ->
                    planIncrementalSplits(table, handle, constraint, session, budget);
            case IcebergTableHandle.READ_MODE_TIME_TRAVEL ->
                    planTimeTravelSplits(table, handle, constraint, budget);
            default -> planFullSplits(table, handle, constraint, session, budget);
        };
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

    /**
     * Plans splits for a full snapshot scan (default mode).
     * Uses the pinned snapshot (if available for this query), the snapshot
     * specified in the handle, or the current snapshot. When a snapshot is
     * resolved and a queryId is present in the session, the snapshot is
     * pinned via {@link IcebergMetadata#pinSnapshotForQuery} to prevent
     * concurrent compaction from removing the underlying data files
     * mid-query.
     *
     * <p>Small, fully enumerated unconstrained scans may reuse immutable split metadata.</p>
     */
    private ConnectorSplitSource planFullSplits(
            Table table, IcebergTableHandle handle, Constraint constraint,
            ConnectorSession session, SplitPlanningBudget budget) {
        Snapshot snapshot = resolveSnapshotWithPin(table, handle, session);
        if (snapshot == null) return new FixedSplitSource(List.of());
        long snapshotId = snapshot.snapshotId();
        boolean cacheable = constraint == null || constraint == Constraint.EMPTY || constraint.getDomains().isEmpty();
        Schema readSchema = IcebergReadSchema.scanSchema(handle, table);
        String tableName = sourceId(handle);
        long targetSize = rangeTargetSize(handle, budget);
        String cacheKey = cacheable ? tableName + "|" + snapshotId + "|" + targetSize + "|"
                + org.pangrova.common.util.DigestUtils.sha256Hex(
                        handle.properties().getOrDefault(IcebergTableHandle.SOURCE_IDENTITY_PROPERTY, "")
                                + org.apache.iceberg.SchemaParser.toJson(readSchema)) : null;
        if (cacheKey != null) {
            List<ConnectorSplit> cached = manifestCache.getIfPresent(cacheKey);
            if (cached != null) {
                if (budget != null) budget.check(cached.size());
                LOG.debug("event=manifest_cache_hit table={} snapshot={} splits={}",
                        tableName, snapshotId, cached.size());
                return new FixedSplitSource(cached);
            }
        }
        TableScan scan = applyConstraintFilter(table.newScan().useSnapshot(snapshotId).project(readSchema), constraint);
        List<ColumnMapping> columnMappings = IcebergTaskToSplit.computeColumnMappings(table, readSchema);
        return new IcebergSplitSource(table.name(), scan::planFiles,
                task -> taskToSplit.convert(task, tableName, "FULL", snapshotId, 0L, 0L, readSchema, columnMappings),
                budget, targetSize, MAX_CACHED_SPLITS_PER_SCAN,
                cacheKey == null ? null : completed -> manifestCache.put(cacheKey, completed));
    }

    private static long rangeTargetSize(IcebergTableHandle handle, SplitPlanningBudget budget) {
        return budget != null && budget.fileGranularity() == SplitPlanningBudget.FileGranularity.BYTE_RANGES
                ? handle.targetScanRangeBytes() : 0;
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
    private ConnectorSplitSource planIncrementalSplits(Table table, IcebergTableHandle handle,
            Constraint constraint, ConnectorSession session, SplitPlanningBudget budget) {
        long fromSnapshot = handle.fromSnapshotId();
        long toSnapshot = handle.toSnapshotId();
        Snapshot target = table.snapshot(toSnapshot);
        Snapshot base = requireAppendChain(table, target, fromSnapshot);
        if (fromSnapshot == toSnapshot) return new FixedSplitSource(List.of());
        pinIfQueryPresent(session, handle, target);
        String tableName = sourceId(handle);
        LOG.debug("event=incremental_scan_start table={}.{} from_snapshot={} to_snapshot={}",
                handle.schemaName(), handle.tableName(), fromSnapshot, toSnapshot);

        return scanIncrementalFiles(table, handle, tableName, base, toSnapshot, constraint, budget);
    }

    private static Snapshot requireAppendChain(Table table, Snapshot target, long from) {
        Snapshot current = target;
        while (current != null && current.snapshotId() != from) {
            if (!org.apache.iceberg.DataOperations.APPEND.equals(current.operation())) {
                throw new IllegalArgumentException("Iceberg changes require an append-only snapshot range");
            }
            Long parent = current.parentId();
            current = parent == null ? null : table.snapshot(parent);
        }
        if (current == null) throw new IllegalArgumentException("Iceberg base snapshot is not a retained ancestor");
        return current;
    }

    private ConnectorSplitSource scanIncrementalFiles(Table table, IcebergTableHandle handle,
            String qualifiedTableName, Snapshot base, long toSnapshot,
            Constraint constraint, SplitPlanningBudget budget) {
        // Get fromSnapshot's sequence number for defensive filtering.
        // In format v2, each data file carries a dataSequenceNumber; files with
        // sequence <= fromSnapshot's sequence existed at or before fromSnapshot
        // and must be excluded from incremental results.
        long fromSnapshot = base.snapshotId();
        long fromSequenceNumber = base.sequenceNumber();

        IncrementalAppendScan scan = applyIncrementalConstraintFilter(table.newIncrementalAppendScan()
                .fromSnapshotExclusive(fromSnapshot).toSnapshot(toSnapshot), constraint);
        Schema readSchema = IcebergReadSchema.scanSchema(handle, table);
        var columnMappings = IcebergTaskToSplit.computeColumnMappings(table, readSchema);
        return new IcebergSplitSource(table.name(), () -> CloseableIterable.filter(scan.planFiles(), task -> {
            // Manifest merges may retain entries already present at the start snapshot.
            Long fileSequence = task.file().dataSequenceNumber();
            return fromSequenceNumber <= 0 || fileSequence == null || fileSequence > fromSequenceNumber;
        }), task -> taskToSplit.convert(task, qualifiedTableName, "INCREMENTAL", toSnapshot,
                fromSnapshot, toSnapshot, readSchema, columnMappings),
                budget, rangeTargetSize(handle, budget), 0, null);
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
    private ConnectorSplitSource planTimeTravelSplits(
            Table table, IcebergTableHandle handle, Constraint constraint, SplitPlanningBudget budget) {
        long snapshotId = handle.snapshotId()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Time-travel read requires a snapshotId on the table handle"));
        validateSnapshotExists(table, snapshotId);
        String tableName = sourceId(handle);
        LOG.debug("event=time_travel_scan_start table={}.{} snapshot_id={}",
                handle.schemaName(), handle.tableName(), snapshotId);

        Schema readSchema = IcebergReadSchema.scanSchema(handle, table);
        TableScan scan = applyConstraintFilter(table.newScan().useSnapshot(snapshotId).project(readSchema), constraint);
        var columnMappings = IcebergTaskToSplit.computeColumnMappings(table, readSchema);
        return new IcebergSplitSource(table.name(), scan::planFiles,
                task -> taskToSplit.convert(task, tableName, "TIME_TRAVEL", snapshotId,
                        0L, 0L, readSchema, columnMappings),
                budget, rangeTargetSize(handle, budget), 0, null);
    }

    private static void validateSnapshotExists(Table table, long snapshotId) {
        if (table.snapshot(snapshotId) == null) {
            throw new IllegalArgumentException(
                    "Snapshot not found: " + snapshotId + " for table: " + table.name());
        }
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

    private String sourceId(IcebergTableHandle handle) {
        return catalogName + "." + handle.schemaName() + "." + handle.tableName();
    }

}
