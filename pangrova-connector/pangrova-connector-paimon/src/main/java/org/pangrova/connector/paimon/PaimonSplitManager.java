package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;
import org.pangrova.connector.paimon.scan.PaimonPartitionExtractor;
import org.pangrova.connector.paimon.scan.PaimonDataFileExtractor;
import org.pangrova.connector.paimon.scan.PaimonNativeMergeAdmission;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.spi.base.ConnectorException;
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
import org.pangrova.connector.spi.ConnectorQueryAuthority;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.FixedSplitSource;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        ConnectorQueryAuthority authority = PaimonSplitRegistry.authority(session);
        try (var permit = authority.enterOperation()) {
            if (table instanceof PaimonBranchTableHandle branches) {
                if (!branches.bound()) {
                    throw new IllegalStateException("Paimon branches must be bound before split planning");
                }
                return new FixedSplitSource(branches.directories().stream()
                        .map(directory -> (ConnectorSplit) new PaimonBranchSystemTableSplit(
                                branches.tableName(), branches.sourceIdentity(), directory))
                        .toList());
            }
            if (table instanceof PaimonFilesTableHandle files) {
                if (!files.bound()) throw new IllegalStateException("Paimon files must be bound before split planning");
                return new FixedSplitSource(files.manifests().stream().map(source -> (ConnectorSplit)
                        new PaimonFilesSystemTableSplit(files.tableName(), source)).toList());
            }
            if (table instanceof PaimonSystemTableHandle system) {
                if (!system.bound()) {
                    throw new IllegalStateException("Paimon system table must be bound before split planning");
                }
                return new FixedSplitSource(system.objects().stream()
                        .map(object -> (ConnectorSplit) new PaimonSystemTableSplit(
                                system.tableName(), system.sourceIdentity(), system.kind(), object))
                        .toList());
            }
            PaimonTableHandle paimonTable = (PaimonTableHandle) table;
            if (paimonTable.incrementalRange().isPresent()) {
                var range = paimonTable.incrementalRange().orElseThrow();
                return getActiveIncrementalSplits(paimonTable,
                        range.fromInclusive(), range.toInclusive(), constraint, authority);
            }
            return getActiveSplits(paimonTable, constraint, authority);
        }
    }

    private ConnectorSplitSource getActiveSplits(PaimonTableHandle paimonTable, Constraint constraint,
            ConnectorQueryAuthority authority) {
        Table freshTable = loadFreshTable(paimonTable);
        FileStoreTable selectedTable = PaimonReadBindings.requireSelectedTable(freshTable, paimonTable);
        PaimonReadBindings.validateSelected(selectedTable, paimonTable);
        if (paimonTable.snapshotId().isPresent()) {
            freshTable = "true".equals(paimonTable.properties().get(PaimonTableHandle.CURRENT_SCHEMA_PROPERTY))
                    ? selectedTable.copyWithoutTimeTravel(Map.of("scan.snapshot-id",
                            Long.toString(paimonTable.snapshotId().orElseThrow())))
                    : applyTimeTravel(freshTable, paimonTable.snapshotId().get());
        } else {
            freshTable = applyTimeTravelHints(freshTable, paimonTable.properties());
        }
        String changelogProducer = resolveChangelogProducer(freshTable);
        TableSplitContext ctx = buildTableContext(freshTable, changelogProducer);
        List<ConnectorSplit> splits = planSplits(freshTable, paimonTable.tableName(), ctx, constraint, authority);
        PaimonReadBindings.validateSelected(selectedTable, paimonTable);
        LOG.debug("event=splits_planned count={} table={}.{} changelog_producer={} partitioned={} snapshotId={}",
                splits.size(), paimonTable.schemaName(), paimonTable.tableName(),
                changelogProducer, !freshTable.partitionKeys().isEmpty(),
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
            Constraint constraint) {
        PaimonTableHandle paimonTable = (PaimonTableHandle) table;
        ConnectorQueryAuthority authority = PaimonSplitRegistry.authority(session);
        try (var permit = authority.enterOperation()) {
            return getActiveIncrementalSplits(paimonTable, fromSnapshotId, toSnapshotId, constraint, authority);
        }
    }

    private ConnectorSplitSource getActiveIncrementalSplits(PaimonTableHandle paimonTable,
            long fromSnapshotId, long toSnapshotId, Constraint constraint,
            ConnectorQueryAuthority authority) {
        Table freshTable = loadFreshTable(paimonTable);
        FileStoreTable selectedTable = PaimonReadBindings.requireSelectedTable(freshTable, paimonTable);
        PaimonReadBindings.validateSelected(selectedTable, paimonTable);
        String tableName = paimonTable.tableName();
        LOG.debug("event=incremental_splits_start table={}.{} from_snapshot={} to_snapshot={}",
                paimonTable.schemaName(), tableName, fromSnapshotId, toSnapshotId);

        List<ConnectorSplit> splits = planIncrementalSplits(
                freshTable, tableName, fromSnapshotId, toSnapshotId, constraint, authority);
        LOG.debug("event=incremental_splits_complete table={}.{} split_count={}",
                paimonTable.schemaName(), tableName, splits.size());
        return new FixedSplitSource(splits);
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
    private static Table applyTimeTravel(Table table, long snapshotId) {
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Time-travel is only supported on primary-key and append-only tables (FileStoreTable), got: "
                            + table.getClass().getSimpleName());
        }
        try {
            org.apache.paimon.Snapshot snapshot = fileStoreTable.snapshotManager().snapshot(snapshotId);
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
        return fileStoreTable.copy(java.util.Map.of("scan.snapshot-id", String.valueOf(snapshotId)));
    }

    /**
     * Creates a time-travel view of the table pinned to a named tag.
     * Uses Paimon's {@code scan.tag-name} option to read from a tagged snapshot.
     *
     * @throws ConnectorException with CONNECTOR_TAG_NOT_FOUND if the tag does not exist
     */
    private static Table applyTimeTravelByTag(Table table, String tagName) {
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Time-travel by tag is only supported on FileStoreTable, got: "
                            + table.getClass().getSimpleName());
        }
        boolean tagExists = fileStoreTable.tagManager().tags().values().stream()
                .anyMatch(names -> names.contains(tagName));
        if (!tagExists) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TAG_NOT_FOUND,
                    "Tag not found: tag-name='" + tagName
                            + "' does not exist in table '" + table.name() + "'");
        }
        return fileStoreTable.copy(java.util.Map.of("scan.tag-name", tagName));
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
    private static Table applyTimeTravelHints(Table table, Map<String, String> properties) {
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
            Table table, String tableName, TableSplitContext ctx, Constraint constraint,
            ConnectorQueryAuthority authority) {
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

        String tableLocation = PaimonDataFileExtractor.extractTableLocation(table);
        List<Split> paimonSplits = readBuilder.newScan().plan().splits();
        String readMode = ctx.primaryKeys.isEmpty() ? "APPEND" : "PRIMARY_KEY";

        if (ctx.needsMerge) {
            return paimonSplits.stream()
                    .map(split -> buildSplit(tableName, split, ctx, table, tableLocation, readMode, authority))
                    .toList();
        }
        // Append-only: expand each DataSplit into one ConnectorSplit per data file
        // so the native reader (one file per split) reads all files.
        return expandPhysicalSplits(tableName, ctx, table, tableLocation, readMode, paimonSplits, authority);
    }

    private List<ConnectorSplit> expandPhysicalSplits(String tableName, TableSplitContext ctx,
            Table table, String tableLocation, String readMode, List<Split> paimonSplits,
            ConnectorQueryAuthority authority) {
        List<ConnectorSplit> result = new ArrayList<>();
        for (Split split : paimonSplits) {
            result.addAll(expandAppendOnlySplit(
                    tableName, split, ctx, table, tableLocation, readMode, authority));
        }
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
            Table table, String tableName,
            long fromSnapshotId, long toSnapshotId, Constraint constraint,
            ConnectorQueryAuthority authority) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Table incrementalView = fileStoreTable.copy(Map.of(
                "incremental-between", fromSnapshotId + "," + toSnapshotId));

        ReadBuilder readBuilder = incrementalView.newReadBuilder();
        Predicate paimonFilter = PaimonFilterBuilder.build(incrementalView.rowType(), constraint);
        if (paimonFilter != null) {
            readBuilder.withFilter(paimonFilter);
        }

        String tableLocation = PaimonDataFileExtractor.extractTableLocation(table);
        String changelogProducer = resolveChangelogProducer(table);
        TableSplitContext ctx = buildTableContext(table, changelogProducer);
        List<Split> paimonSplits = readBuilder.newScan().plan().splits();

        // Incremental splits are append-only — expand per-file
        List<ConnectorSplit> result = new ArrayList<>();
        for (Split split : paimonSplits) {
            result.addAll(expandAppendOnlySplit(
                    tableName, split, ctx, incrementalView, tableLocation, "INCREMENTAL", authority));
        }
        return List.copyOf(result);
    }

    private List<ConnectorSplit> expandAppendOnlySplit(
            String tableName, Split paimonSplit,
            TableSplitContext ctx, Table table, String tableLocation, String readMode,
            ConnectorQueryAuthority authority) {
        if (!(paimonSplit instanceof DataSplit dataSplit)) {
            return List.of(buildSplit(
                    tableName, paimonSplit, ctx, table, tableLocation, readMode, authority));
        }
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        if (dataFiles == null || dataFiles.isEmpty()) {
            return List.of();
        }
        requireDataSplit(dataSplit);
        if (dataFiles.size() == 1) {
            return List.of(buildSplit(
                    tableName, paimonSplit, ctx, table, tableLocation, readMode, authority));
        }
        int bucketId = dataSplit.bucket();
        Map<String, String> partitionValues =
                PaimonPartitionExtractor.extractPartitionValues(paimonSplit, table);
        List<ConnectorSplit> splits = new ArrayList<>(dataFiles.size());
        for (DataFileMeta file : dataFiles) {
            DataSplit fileSplit = dataSplit.filterDataFile(file::equals).orElseThrow();
            if (fileSplit.dataFiles().size() != 1) {
                throw new IllegalStateException(
                        "SDK split contains duplicate data file identities: " + file.fileName());
            }
            long registryId = splitRegistry.register(authority, table, fileSplit);
            String filePath = PaimonDataFileExtractor.buildFilePath(dataSplit, file);
            var schemaEvolution = PaimonPartitionExtractor.extractSchemaEvolution(file, table);
            PaimonLsmLevel singleLevel = new PaimonLsmLevel(
                    file.level(), List.of(new PaimonLsmFile(physicalFile(filePath, file),
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
                    physicalFile(filePath, file), ctx.mergePolicy));
        }
        return splits;
    }

    private ConnectorSplit buildSplit(
            String tableName, Split paimonSplit,
            TableSplitContext ctx, Table table, String tableLocation, String readMode,
            ConnectorQueryAuthority authority) {
        int bucketId = extractBucketId(paimonSplit);
        Map<String, String> partitionValues =
                PaimonPartitionExtractor.extractPartitionValues(paimonSplit, table);

        if (!partitionValues.isEmpty()) {
            LOG.debug("event=paimon_partition_extracted table={} bucket={} partitions={}",
                    tableName, bucketId, partitionValues);
        }

        DataSplit dataSplit = requireDataSplit(paimonSplit);
        DataFileMeta primary = dataSplit.dataFiles().get(0);
        var schemaEvolution = PaimonPartitionExtractor.extractSchemaEvolution(primary, table);
        String primaryFilePath = PaimonDataFileExtractor.buildFilePath(dataSplit, primary);
        List<PaimonLsmLevel> lsmLevels =
                PaimonDataFileExtractor.extractLsmLevels(table, paimonSplit);

        long registryId = splitRegistry.register(authority, table, paimonSplit);
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

    private static int extractBucketId(Split split) {
        if (split instanceof org.apache.paimon.table.source.DataSplit dataSplit) {
            return dataSplit.bucket();
        }
        return -1;
    }

    // ── Table context ──

    private static TableSplitContext buildTableContext(Table table, String changelogProducer) {
        Map<String, String> options = table.options();
        List<String> primaryKeys = table.primaryKeys();
        String mergeEngine = options.getOrDefault("merge-engine", "DEDUPLICATE")
                .replace("-", "_").toUpperCase();
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
        return value.replace("-", "_").toUpperCase();
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
