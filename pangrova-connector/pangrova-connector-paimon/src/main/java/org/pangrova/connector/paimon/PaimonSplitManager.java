package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;
import org.pangrova.connector.paimon.scan.PaimonPartitionExtractor;
import org.pangrova.connector.paimon.scan.PaimonPartitionExtractor.SchemaEvolution;
import org.pangrova.connector.paimon.scan.PaimonDataFileExtractor;
import org.pangrova.connector.paimon.scan.PaimonNativeMergeAdmission;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonLsmLevel;
import org.pangrova.connector.paimon.handle.PaimonLsmFile;
import org.pangrova.connector.paimon.handle.PaimonPhysicalFile;
import org.pangrova.connector.paimon.handle.PaimonMergePolicy;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonSystemTableHandle;
import org.pangrova.connector.paimon.handle.PaimonFilesTableHandle;
import org.pangrova.connector.paimon.handle.PaimonFilesSystemTableSplit;
import org.pangrova.connector.paimon.handle.PaimonSystemTableSplit;
import org.pangrova.connector.paimon.handle.PaimonBranchTableHandle;
import org.pangrova.connector.paimon.handle.PaimonBranchSystemTableSplit;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.runtime.scan.FixedSplitSource;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Paimon split manager that plans scan splits from table snapshots.
 *
 * <p>Delegates filter construction to {@link PaimonFilterBuilder} and
 * partition/schema-evolution extraction to {@link PaimonPartitionExtractor}.</p>
 */
public class PaimonSplitManager implements ConnectorSplitManager {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSplitManager.class);

    private final Catalog catalog;
    private final PaimonSplitRegistry splitRegistry;

    public PaimonSplitManager(Catalog catalog, PaimonSplitRegistry splitRegistry) {
        this.catalog = catalog;
        this.splitRegistry = splitRegistry;
    }

    @Override
    public ConnectorSplitSource getBoundSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorMetadata metadata, ConnectorReadBinding binding,
            DynamicFilter dynamicFilter, Constraint constraint, SplitPlanningBudget budget) {
        budget.check(0);
        if (binding.state() == ReadSnapshotState.EMPTY) {
            throw new IllegalArgumentException("An empty binding has no splits to enumerate");
        }
        if (!(binding.handle() instanceof PaimonTableHandle handle)
                || !(metadata instanceof PaimonMetadata paimonMetadata)
                || !paimonMetadata.ownsReadContext(catalog, splitRegistry)) {
            metadata.validateReadBinding(session, binding);
            return getSplits(session, binding.handle(), constraint, budget);
        }
        ConnectorQueryAuthority authority = PaimonSplitRegistry.authority(session);
        try (var permit = authority.enterOperation()) {
            splitRegistry.requireActiveQuery(authority);
            return planPhysicalSplits(handle, constraint, null, binding.sourceIdentity(),
                    scanBudget(budget, authority));
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        return getSplits(session, table, constraint, SplitPlanningBudget.standard());
    }

    private ConnectorSplitSource getSplits(ConnectorSession session, ConnectorTableHandle table,
            Constraint constraint, SplitPlanningBudget budget) {
        ConnectorQueryAuthority authority = PaimonSplitRegistry.authority(session);
        try (var permit = authority.enterOperation()) {
            if (table instanceof PaimonBranchTableHandle branches) {
                if (!branches.bound()) {
                    throw new IllegalStateException("Paimon branches must be bound before split planning");
                }
                budget.check(branches.directories().size());
                return new FixedSplitSource(branches.directories().stream()
                        .map(directory -> (ConnectorSplit) new PaimonBranchSystemTableSplit(
                                branches.tableName(), branches.sourceIdentity(), directory))
                        .toList());
            }
            if (table instanceof PaimonFilesTableHandle files) {
                if (!files.bound()) throw new IllegalStateException("Paimon files must be bound before split planning");
                budget.check(files.manifests().size());
                return new FixedSplitSource(files.manifests().stream().map(source -> (ConnectorSplit)
                        new PaimonFilesSystemTableSplit(files.tableName(), source)).toList());
            }
            if (table instanceof PaimonSystemTableHandle system) {
                if (!system.bound()) {
                    throw new IllegalStateException("Paimon system table must be bound before split planning");
                }
                budget.check(system.objects().size());
                return new FixedSplitSource(system.objects().stream()
                        .map(object -> (ConnectorSplit) new PaimonSystemTableSplit(
                                system.tableName(), system.sourceIdentity(), system.kind(), object))
                        .toList());
            }
            return planPhysicalSplits((PaimonTableHandle) table, constraint, authority, null,
                    scanBudget(budget, authority));
        }
    }

    private ConnectorSplitSource planPhysicalSplits(PaimonTableHandle handle, Constraint constraint,
            ConnectorQueryAuthority sdkReaderAuthority, String bindingIdentity, PaimonScanBudget planning) {
        planning.check(0);
        FileStoreTable table = PaimonReadBindings.requireSelectedTable(loadFreshTable(handle), handle);
        PaimonReadBindings.validateSelected(table, handle, bindingIdentity);
        if (handle.incrementalRange().isPresent()) {
            var range = handle.incrementalRange().orElseThrow();
            return getActiveIncrementalSplits(table, handle,
                    range.fromInclusive(), range.toInclusive(), constraint, sdkReaderAuthority, planning);
        }
        return getActiveSplits(table, handle, constraint, sdkReaderAuthority, planning);
    }

    private ConnectorSplitSource getActiveSplits(FileStoreTable selectedTable, PaimonTableHandle paimonTable,
            Constraint constraint, ConnectorQueryAuthority sdkReaderAuthority, PaimonScanBudget planning) {
        FileStoreTable scanTable;
        if (paimonTable.snapshotId().isPresent()) {
            scanTable = "true".equals(paimonTable.properties().get(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY))
                    ? selectedTable.copyWithoutTimeTravel(Map.of("scan.snapshot-id",
                            Long.toString(paimonTable.snapshotId().orElseThrow())))
                    : applyTimeTravel(selectedTable, paimonTable.snapshotId().orElseThrow());
        } else {
            scanTable = applyTimeTravelHints(selectedTable, paimonTable.properties());
        }
        String changelogProducer = resolveChangelogProducer(scanTable);
        TableSplitContext ctx = buildTableContext(scanTable, changelogProducer);
        List<ConnectorSplit> splits = planSplits(scanTable, paimonTable.tableName(), ctx, constraint,
                sdkReaderAuthority, planning);
        PaimonReadBindings.validateSelected(selectedTable, paimonTable);
        LOG.debug("event=splits_planned count={} table={}.{} changelog_producer={} partitioned={} snapshotId={}",
                splits.size(), paimonTable.schemaName(), paimonTable.tableName(),
                changelogProducer, !scanTable.partitionKeys().isEmpty(),
                paimonTable.snapshotId().orElse(-1L));
        return new FixedSplitSource(splits);
    }

    /**
     * Returns only the data files added between two snapshots using
     * Paimon's incremental-between scan mode.
     *
     * <p>Creates a copy of the table with {@code incremental-between}
     * configured to the given snapshot range, then plans splits from
     * that incremental view. This enables O(delta) refresh for Dynamic
     * Tables backed by Paimon.</p>
     */
    @Override
    public ConnectorSplitSource getIncrementalSplits(
            ConnectorSession session,
            ConnectorTableHandle table,
            long fromSnapshotId,
            long toSnapshotId,
            Constraint constraint,
            SplitPlanningBudget budget) {
        PaimonTableHandle paimonTable = (PaimonTableHandle) table;
        ConnectorQueryAuthority authority = PaimonSplitRegistry.authority(session);
        try (var permit = authority.enterOperation()) {
            var planning = scanBudget(budget, authority);
            planning.check(0);
            FileStoreTable selected = PaimonReadBindings.requireSelectedTable(loadFreshTable(paimonTable), paimonTable);
            PaimonReadBindings.validateSelected(selected, paimonTable);
            return getActiveIncrementalSplits(selected, paimonTable, fromSnapshotId, toSnapshotId,
                    constraint, authority, planning);
        }
    }

    private ConnectorSplitSource getActiveIncrementalSplits(FileStoreTable selectedTable, PaimonTableHandle paimonTable,
            long fromSnapshotId, long toSnapshotId, Constraint constraint,
            ConnectorQueryAuthority sdkReaderAuthority, PaimonScanBudget planning) {
        String tableName = paimonTable.tableName();
        LOG.debug("event=incremental_splits_start table={}.{} from_snapshot={} to_snapshot={}",
                paimonTable.schemaName(), tableName, fromSnapshotId, toSnapshotId);

        List<ConnectorSplit> splits = planIncrementalSplits(
                selectedTable, tableName, fromSnapshotId, toSnapshotId, constraint, sdkReaderAuthority, planning);
        LOG.debug("event=incremental_splits_complete table={}.{} split_count={}",
                paimonTable.schemaName(), tableName, splits.size());
        return new FixedSplitSource(splits);
    }

    private static PaimonScanBudget scanBudget(SplitPlanningBudget stage, ConnectorQueryAuthority authority) {
        return new PaimonScanBudget(stage, authority::isClosed, PaimonScanBudget.MAX_METADATA_CANDIDATES);
    }

    private Table loadFreshTable(PaimonTableHandle handle) {
        Identifier id = Identifier.create(handle.schemaName(), handle.tableName());
        try {
            return PaimonTableIdentity.loadFresh(catalog, id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + id, e);
        }
    }

    /**
     * Creates a time-travel view of the table pinned to a specific snapshot.
     * Uses Paimon's {@code scan.snapshot-id} option to read from a historical snapshot.
     *
     * @throws ConnectorException with CONNECTOR_SNAPSHOT_NOT_FOUND if the snapshot does not exist
     */
    private static FileStoreTable applyTimeTravel(FileStoreTable table, long snapshotId) {
        try {
            org.apache.paimon.Snapshot snapshot = table.snapshotManager().snapshot(snapshotId);
            if (snapshot == null) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                        "Snapshot not found: snapshot-id=" + snapshotId
                                + " does not exist in table '" + table.name() + "'");
            }
        } catch (ConnectorException ce) {
            throw ce;
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                    "Snapshot not found: snapshot-id=" + snapshotId
                            + " does not exist in table '" + table.name() + "'", e);
        }
        return table.copy(Map.of("scan.snapshot-id", Long.toString(snapshotId)));
    }

    /**
     * Creates a time-travel view of the table pinned to a named tag.
     * Uses Paimon's {@code scan.tag-name} option to read from a tagged snapshot.
     *
     * @throws ConnectorException with CONNECTOR_TAG_NOT_FOUND if the tag does not exist
     */
    private static FileStoreTable applyTimeTravelByTag(FileStoreTable table, String tagName) {
        boolean tagExists = table.tagManager().tags().values().stream()
                .anyMatch(names -> names.contains(tagName));
        if (!tagExists) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TAG_NOT_FOUND,
                    "Tag not found: tag-name='" + tagName
                            + "' does not exist in table '" + table.name() + "'");
        }
        return table.copy(Map.of("scan.tag-name", tagName));
    }

    /**
     * Applies time-travel hints from table properties if present.
     *
     * <p>Supports two hint keys (mutually exclusive, snapshot-id takes precedence):
     * <ul>
     *   <li>{@code scan.snapshot-id} — pin to a specific snapshot ID</li>
     *   <li>{@code scan.tag-name} — pin to a named tag</li>
     * </ul>
     *
     * @param table the fresh table loaded from catalog
     * @param properties the table handle properties (from query hints)
     * @return the table, possibly wrapped with time-travel configuration
     * @throws ConnectorException if snapshot-id is not a valid number or does not exist
     */
    private static FileStoreTable applyTimeTravelHints(FileStoreTable table, Map<String, String> properties) {
        String snapshotIdHint = properties.get("scan.snapshot-id");
        String tagNameHint = properties.get("scan.tag-name");

        if (snapshotIdHint != null && !snapshotIdHint.isBlank()) {
            long snapshotId;
            try {
                snapshotId = Long.parseLong(snapshotIdHint.trim());
            } catch (NumberFormatException e) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                        "Invalid scan.snapshot-id hint: '" + snapshotIdHint
                                + "' is not a valid snapshot ID (must be a numeric value)", e);
            }
            return applyTimeTravel(table, snapshotId);
        }

        if (tagNameHint != null && !tagNameHint.isBlank()) {
            return applyTimeTravelByTag(table, tagNameHint.trim());
        }

        return table;
    }

    private List<ConnectorSplit> planSplits(
            FileStoreTable table, String tableName, TableSplitContext ctx, Constraint constraint,
            ConnectorQueryAuthority sdkReaderAuthority, PaimonScanBudget planning) {
        ReadBuilder readBuilder = table.newReadBuilder();

        Predicate paimonFilter = PaimonFilterBuilder.build(table.rowType(), constraint);
        if (paimonFilter != null) {
            readBuilder.withFilter(paimonFilter);
            LOG.debug("event=paimon_predicate_pushdown table={}", tableName);
        }

        // Consumer-id passthrough: when configured, Paimon tracks consumption
        // progress per consumer and resumes from the last committed snapshot.
        String consumerId = table.options().get("consumer-id");
        if (consumerId != null && !consumerId.isBlank()) {
            LOG.debug("event=consumer_id_passthrough table={} consumer_id={}", tableName, consumerId);
        }

        // Sequence.field passthrough: when configured, Paimon uses the specified
        // field to determine merge order instead of file sequence number.
        String sequenceField = table.options().get("sequence.field");
        if (sequenceField != null && !sequenceField.isBlank()) {
            LOG.debug("event=sequence_field_configured table={} sequence_field={}", tableName, sequenceField);
        }

        String readMode = ctx.primaryKeys.isEmpty() ? "APPEND" : "PRIMARY_KEY";
        return convertSplits(tableName, ctx, table, readMode, planning.plan(readBuilder),
                sdkReaderAuthority, planning, ctx.needsMerge);
    }

    private List<ConnectorSplit> convertSplits(String tableName, TableSplitContext ctx,
            FileStoreTable table, String readMode, List<Split> paimonSplits,
            ConnectorQueryAuthority sdkReaderAuthority, PaimonScanBudget planning, boolean merge) {
        long count = 0;
        for (Split split : paimonSplits) {
            if (!merge && split instanceof DataSplit data) {
                count += data.dataFiles() == null ? 0 : data.dataFiles().size();
            } else {
                count++;
            }
            planning.check(count);
        }
        var evolution = new SchemaEvolution(table);
        List<ConnectorSplit> result = new ArrayList<>((int) count);
        for (Split split : paimonSplits) {
            planning.check(result.size());
            if (merge) result.add(buildSplit(tableName, split, ctx, table, evolution, readMode, sdkReaderAuthority));
            else expandPhysicalSplit(result, tableName, split, ctx, table, evolution, readMode,
                    sdkReaderAuthority, planning);
        }
        planning.check(result.size());
        return List.copyOf(result);
    }

    /**
     * Plans splits for the incremental snapshot range using Paimon's
     * {@code incremental-between} scan mode.
     *
     * <p>Creates a table copy configured with the snapshot range, then
     * plans and expands splits the same way as a full scan. Incremental
     * splits are always append-only (the caller verifies via
     * {@link IncrementalScanCapability#isAppendOnly} before calling),
     * so we expand per-file for native reader compatibility.</p>
     */
    private List<ConnectorSplit> planIncrementalSplits(
            FileStoreTable table, String tableName,
            long fromSnapshotId, long toSnapshotId, Constraint constraint,
            ConnectorQueryAuthority sdkReaderAuthority, PaimonScanBudget planning) {
        planning.checkSnapshotRange(fromSnapshotId, toSnapshotId);
        FileStoreTable incrementalView = table.copy(Map.of(
                "incremental-between", fromSnapshotId + "," + toSnapshotId));

        ReadBuilder readBuilder = incrementalView.newReadBuilder();
        Predicate paimonFilter = PaimonFilterBuilder.build(incrementalView.rowType(), constraint);
        if (paimonFilter != null) {
            readBuilder.withFilter(paimonFilter);
        }

        String changelogProducer = resolveChangelogProducer(table);
        TableSplitContext ctx = buildTableContext(table, changelogProducer);
        return convertSplits(tableName, ctx, incrementalView, "INCREMENTAL",
                planning.plan(readBuilder), sdkReaderAuthority, planning, false);
    }

    private void expandPhysicalSplit(List<ConnectorSplit> splits, String tableName, Split paimonSplit,
            TableSplitContext ctx, Table table, SchemaEvolution evolution, String readMode,
            ConnectorQueryAuthority sdkReaderAuthority, PaimonScanBudget planning) {
        if (!(paimonSplit instanceof DataSplit dataSplit)) {
            splits.add(buildSplit(tableName, paimonSplit, ctx, table, evolution, readMode, sdkReaderAuthority));
            return;
        }
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        if (dataFiles == null || dataFiles.isEmpty()) return;
        requireDataSplit(dataSplit);
        if (dataFiles.size() == 1) {
            splits.add(buildSplit(tableName, paimonSplit, ctx, table, evolution, readMode, sdkReaderAuthority));
            return;
        }
        int bucketId = dataSplit.bucket();
        Map<String, String> partitionValues =
                PaimonPartitionExtractor.extractPartitionValues(paimonSplit, table);
        Set<DataFileMeta> seenFiles = new HashSet<>();
        for (DataFileMeta file : dataFiles) {
            planning.check(splits.size());
            if (!seenFiles.add(file)) {
                throw new IllegalStateException(
                        "SDK split contains duplicate data file identities: " + file.fileName());
            }
            long registryId = sdkReaderAuthority == null ? PaimonSplit.NO_SDK_READER
                    : splitRegistry.register(sdkReaderAuthority, table,
                            dataSplit.filterDataFile(file::equals).orElseThrow());
            String filePath = PaimonDataFileExtractor.buildFilePath(dataSplit, file);
            PaimonPhysicalFile physical = physicalFile(filePath, file);
            var schemaEvolution = evolution.forSource(file.schemaId());
            PaimonLsmLevel singleLevel = new PaimonLsmLevel(
                    file.level(), List.of(new PaimonLsmFile(physical,
                            file.minSequenceNumber(), file.maxSequenceNumber(),
                            PaimonDataFileExtractor.deletionVector(dataSplit, file),
                            schemaEvolution.columnMappings(), schemaEvolution.typePromotions())), file.rowCount());
            splits.add(new PaimonSplit(
                    tableName, registryId, file.rowCount(),
                    bucketId, ctx.primaryKeys, ctx.mergeEngine, ctx.changelogProducer,
                    dataSplit.snapshotId(), 0L, 0L, readMode, List.of(singleLevel),
                    false, ctx.deletionVectorsEnabled,
                    ctx.aggregationSpecs, ctx.changelogMode, partitionValues,
                    schemaEvolution.columnMappings(), schemaEvolution.typePromotions(),
                    physical, ctx.mergePolicy));
        }
    }

    private ConnectorSplit buildSplit(
            String tableName, Split paimonSplit,
            TableSplitContext ctx, Table table, SchemaEvolution evolution, String readMode,
            ConnectorQueryAuthority sdkReaderAuthority) {
        DataSplit dataSplit = requireDataSplit(paimonSplit);
        int bucketId = dataSplit.bucket();
        Map<String, String> partitionValues =
                PaimonPartitionExtractor.extractPartitionValues(paimonSplit, table);

        if (!partitionValues.isEmpty()) {
            LOG.debug("event=paimon_partition_extracted table={} bucket={} partitions={}",
                    tableName, bucketId, partitionValues);
        }

        DataFileMeta primary = dataSplit.dataFiles().get(0);
        var schemaEvolution = evolution.forSource(primary.schemaId());
        String primaryFilePath = PaimonDataFileExtractor.buildFilePath(dataSplit, primary);
        List<PaimonLsmLevel> lsmLevels =
                PaimonDataFileExtractor.extractLsmLevels(evolution, dataSplit);

        long registryId = sdkReaderAuthority == null ? PaimonSplit.NO_SDK_READER
                : splitRegistry.register(sdkReaderAuthority, table, paimonSplit);
        return new PaimonSplit(
                tableName, registryId, paimonSplit.rowCount(),
                bucketId, ctx.primaryKeys, ctx.mergeEngine, ctx.changelogProducer,
                dataSplit.snapshotId(), 0L, 0L, readMode, lsmLevels,
                ctx.needsMerge, ctx.deletionVectorsEnabled,
                ctx.aggregationSpecs, ctx.changelogMode, partitionValues,
                schemaEvolution.columnMappings(), schemaEvolution.typePromotions(),
                physicalFile(primaryFilePath, primary), ctx.mergePolicy);
    }

    private static DataSplit requireDataSplit(Split split) {
        if (!(split instanceof DataSplit data) || data.dataFiles().isEmpty() || data.snapshotId() <= 0) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Native Paimon scan requires physical files from an exact SDK snapshot");
        }
        return data;
    }

    private static PaimonPhysicalFile physicalFile(String path, DataFileMeta file) {
        return new PaimonPhysicalFile(path, file.fileFormat(), file.fileSize(), file.rowCount(), file.schemaId());
    }

    // ── Table context ──

    private static TableSplitContext buildTableContext(Table table, String changelogProducer) {
        Map<String, String> options = table.options();
        List<String> primaryKeys = table.primaryKeys();
        String mergeEngine = options.getOrDefault("merge-engine", "DEDUPLICATE")
                .replace("-", "_").toUpperCase(Locale.ROOT);
        boolean deletionVectorsEnabled = "true".equalsIgnoreCase(
                options.getOrDefault("deletion-vectors.enabled", "false"));
        boolean changelogMode = isChangelogCapable(changelogProducer);
        boolean needsMerge = !primaryKeys.isEmpty() && !deletionVectorsEnabled;
        if (needsMerge && "PARTIAL_UPDATE".equals(mergeEngine)) {
            PaimonNativeMergeAdmission.validatePartialUpdate(table);
        }

        Map<String, String> aggregationSpecs = Map.of();
        if ("AGGREGATION".equals(mergeEngine)) {
            aggregationSpecs = extractAggregationSpecs(table);
        }

        return new TableSplitContext(
                primaryKeys, mergeEngine, changelogProducer,
                needsMerge, deletionVectorsEnabled, changelogMode,
                aggregationSpecs, mergePolicy(options, mergeEngine));
    }

    private static PaimonMergePolicy mergePolicy(Map<String, String> options, String engine) {
        var sdkOptions = org.apache.paimon.options.Options.fromMap(options);
        boolean ignore = sdkOptions.get(org.apache.paimon.CoreOptions.IGNORE_DELETE);
        boolean remove = switch (engine) {
            case "PARTIAL_UPDATE" ->
                    sdkOptions.get(org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE);
            case "AGGREGATION" -> sdkOptions.get(org.apache.paimon.CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE);
            default -> false;
        };
        return new PaimonMergePolicy(ignore, remove);
    }

    private static Map<String, String> extractAggregationSpecs(Table table) {
        Map<String, String> specs = new LinkedHashMap<>();
        var options = new org.apache.paimon.CoreOptions(org.apache.paimon.options.Options.fromMap(table.options()));
        for (String name : table.rowType().getFieldNames()) {
            if (!table.primaryKeys().contains(name)) {
                String function = org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction.getAggFuncName(
                        name, options, table.primaryKeys(), options.sequenceField());
                String normalized = function.toUpperCase(java.util.Locale.ROOT);
                PaimonNativeMergeAdmission.validateAggregation(name, normalized,
                        table.rowType().getTypeAt(table.rowType().getFieldNames().indexOf(name)), options);
                specs.put(name, normalized);
            }
        }
        return Map.copyOf(specs);
    }

    private static String resolveChangelogProducer(Table table) {
        String value = table.options().getOrDefault("changelog-producer", "NONE");
        return value.replace("-", "_").toUpperCase(Locale.ROOT);
    }

    private static boolean isChangelogCapable(String changelogProducer) {
        return "LOOKUP".equals(changelogProducer)
                || "FULL_COMPACTION".equals(changelogProducer);
    }

    private record TableSplitContext(
            List<String> primaryKeys,
            String mergeEngine,
            String changelogProducer,
            boolean needsMerge,
            boolean deletionVectorsEnabled,
            boolean changelogMode,
            Map<String, String> aggregationSpecs,
            PaimonMergePolicy mergePolicy) {}
}
