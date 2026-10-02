package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.scan.PaimonDataFileExtractor;

import org.pangrova.connector.paimon.index.PaimonSnapshotAwareMetadata;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexType;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotDataFile;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Index-related metadata accessors for the Paimon connector: data-file
 * listing, file-level statistics, and snapshot-aware metadata.
 */
final class PaimonIndexAccessors {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonIndexAccessors.class);

    private PaimonIndexAccessors() {}

    /**
     * Paimon provides MIN_MAX for all columns (manifest + data file statistics)
     * and BLOOM_FILTER, BITMAP, BSI conditionally based on table properties.
     */
    static List<NativeIndexCapability> nativeIndexCapabilities() {
        return List.of(
                new NativeIndexCapability(
                        NativeIndexType.MIN_MAX,
                        List.of("*"),
                        "Paimon manifest statistics + data file min/max"),
                new NativeIndexCapability(
                        NativeIndexType.BLOOM_FILTER,
                        List.of(),
                        "Paimon file-index bloom filters for configured columns"),
                new NativeIndexCapability(
                        NativeIndexType.BITMAP,
                        List.of(),
                        "Paimon file-index bitmap for configured columns"),
                new NativeIndexCapability(
                        NativeIndexType.BSI,
                        List.of(),
                        "Paimon file-index BSI for configured numeric columns")
        );
    }

    /** Lists the physical data-file paths from one immutable current snapshot. */
    static List<String> listDataFiles(Catalog catalog, String schemaName, String tableName) {
        try {
            Identifier tableId = Identifier.create(schemaName, tableName);
            Table table = catalog.getTable(tableId);
            PaimonSnapshotAwareMetadata snapshotMetadata =
                    new PaimonSnapshotAwareMetadata(table);
            Optional<String> snapshot = snapshotMetadata.currentSnapshotId();
            return snapshot.isEmpty()
                    ? List.of()
                    : snapshotMetadata.listFilesAtSnapshot(snapshot.get()).stream()
                            .map(SnapshotDataFile::path)
                            .toList();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to list complete Paimon data files for "
                            + schemaName + "." + tableName, e);
        }
    }

    /** Returns snapshot-aware metadata for the table, or empty on failure. */
    static Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(
            Catalog catalog, String schemaName, String tableName) {
        try {
            Table table = catalog.getTable(Identifier.create(schemaName, tableName));
            return table instanceof FileStoreTable
                    ? Optional.of(new PaimonSnapshotAwareMetadata(table))
                    : Optional.empty();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve Paimon snapshot metadata for "
                            + schemaName + "." + tableName, e);
        }
    }

    /** Reads file-level statistics for the requested data file paths. */
    static List<DataFileStatistics> getFileStatistics(
            Catalog catalog, String schemaName, String tableName, List<String> dataFilePaths) {
        try {
            Identifier tableId = Identifier.create(schemaName, tableName);
            Table table = catalog.getTable(tableId);
            if (!(table instanceof FileStoreTable)) {
                return List.of();
            }
            Set<String> targetPaths = new HashSet<>(dataFilePaths);
            String tableLocation = PaimonDataFileExtractor.extractTableLocation(table);
            List<DataField> fields = table.rowType().getFields();

            List<DataFileStatistics> result = new ArrayList<>();
            for (Split split : table.newReadBuilder().newScan().plan().splits()) {
                if (!(split instanceof DataSplit dataSplit)) {
                    continue;
                }
                collectMatchingFileStats(tableLocation, dataSplit, fields,
                        targetPaths, result);
            }
            return result;
        } catch (Exception e) {
            LOG.warn("action=get_file_statistics_failed schema={} table={} error={}",
                    schemaName, tableName, e.getMessage());
            return List.of();
        }
    }

    private static void collectMatchingFileStats(String tableLocation, DataSplit dataSplit,
                                                 List<DataField> fields, Set<String> targetPaths,
                                                 List<DataFileStatistics> result) {
        for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
            String filePath = PaimonDataFileExtractor.buildFilePath(dataSplit, fileMeta);
            if (!targetPaths.contains(filePath)) {
                continue;
            }
            List<FileColumnStatistics> colStats = extractPaimonColumnStats(fileMeta, fields);
            result.add(new DataFileStatistics(
                    filePath, String.valueOf(fileMeta.creationTime()),
                    fileMeta.rowCount(), fileMeta.fileSize(), colStats));
        }
    }

    private static List<FileColumnStatistics> extractPaimonColumnStats(
            DataFileMeta fileMeta, List<DataField> fields) {
        List<FileColumnStatistics> stats = new ArrayList<>();
        SimpleStats valueStats = fileMeta.valueStats();
        if (valueStats == null) {
            return stats;
        }
        InternalRow minValues = valueStats.minValues();
        InternalRow maxValues = valueStats.maxValues();
        InternalArray nullCounts = valueStats.nullCounts();
        if (minValues == null || maxValues == null) {
            return stats;
        }
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            try {
                FileColumnStatistics fieldStats = buildFieldStats(
                        fileMeta, field, minValues, maxValues, nullCounts, i);
                if (fieldStats != null) {
                    stats.add(fieldStats);
                }
            } catch (Exception e) {
                LOG.debug("action=column_stats_skip column={} type={} error={}",
                        field.name(), field.type(), e.getMessage());
            }
        }
        return stats;
    }

    private static FileColumnStatistics buildFieldStats(
            DataFileMeta fileMeta, DataField field, InternalRow minValues,
            InternalRow maxValues, InternalArray nullCounts, int index) {
        Comparable<?> minVal = PaimonTypeMapper.extractComparable(minValues, index, field.type());
        Comparable<?> maxVal = PaimonTypeMapper.extractComparable(maxValues, index, field.type());
        if (minVal == null || maxVal == null) {
            return null;
        }
        long nullCount = nullCounts != null && index < nullCounts.size()
                ? nullCounts.getLong(index) : 0L;
        return new FileColumnStatistics(
                field.name(), minVal, maxVal, nullCount,
                fileMeta.rowCount(), 0L, 0L);
    }
}
