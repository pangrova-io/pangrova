package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.fs.IcebergCatalogColumnValueReader;
import org.pangrova.connector.iceberg.fs.IcebergFileSystemAccessor;
import org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator;
import org.pangrova.connector.iceberg.index.IcebergSnapshotAwareMetadata;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.ConnectorIndexSupport;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.connector.index.NativeIndexType;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotDataFile;
import org.pangrova.spi.connector.index.SnapshotPinProvider;
import org.pangrova.spi.connector.index.statistics.DataFileStatistics;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Bundles all index-related capabilities for the Iceberg connector: file
 * system access, column value reads, native (min/max + bloom) index
 * evaluation, snapshot-aware metadata, data file listing, and per-file
 * statistics extraction.
 */
final class IcebergIndexAccessors implements ConnectorIndexSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergIndexAccessors.class);

    private final Catalog catalog;
    private final FileIO fileIO;
    private final IcebergNativeIndexEvaluator nativeIndexEvaluator;
    private final IcebergCommitNotifier notifier;

    IcebergIndexAccessors(Catalog catalog, FileIO fileIO,
                          IcebergNativeIndexEvaluator nativeIndexEvaluator,
                          IcebergCommitNotifier notifier) {
        this.catalog = catalog;
        this.fileIO = fileIO;
        this.nativeIndexEvaluator = nativeIndexEvaluator;
        this.notifier = notifier;
    }

    @Override
    public Optional<FileSystemAccessor> getFileSystemAccessor() {
        if (fileIO == null) {
            return Optional.empty();
        }
        return Optional.of(new IcebergFileSystemAccessor(fileIO));
    }

    @Override
    public Optional<ColumnValueReader> getColumnValueReader() {
        if (fileIO == null) {
            return Optional.empty();
        }
        return Optional.of(new IcebergCatalogColumnValueReader(catalog, fileIO));
    }

    @Override
    public List<NativeIndexCapability> nativeIndexCapabilities() {
        // Iceberg provides MIN_MAX for all columns (manifest bounds + Parquet footer)
        // and BLOOM_FILTER for configured columns
        return List.of(
                new NativeIndexCapability(
                        NativeIndexType.MIN_MAX,
                        List.of("*"),
                        "Iceberg manifest bounds + Parquet footer min/max statistics"),
                new NativeIndexCapability(
                        NativeIndexType.BLOOM_FILTER,
                        List.of(),
                        "Parquet SBBF bloom filters for configured columns")
        );
    }

    @Override
    public Optional<NativeIndexEvaluator> getNativeIndexEvaluator() {
        return Optional.of(nativeIndexEvaluator);
    }

    @Override
    public Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return Optional.of(new IcebergSnapshotAwareMetadata(catalog, schemaName, tableName));
    }

    @Override
    public Optional<SnapshotPinProvider> getSnapshotPinProvider() {
        return Optional.of(new IcebergSnapshotPinProvider(catalog));
    }

    @Override
    public List<String> listDataFiles(String catalogName, String schemaName, String tableName) {
        try {
            IcebergSnapshotAwareMetadata metadata =
                    new IcebergSnapshotAwareMetadata(catalog, schemaName, tableName);
            Optional<String> snapshot = metadata.currentSnapshotId();
            return snapshot.isEmpty()
                    ? List.of()
                    : metadata.listFilesAtSnapshot(snapshot.get()).stream()
                            .map(SnapshotDataFile::path)
                            .toList();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to list complete Iceberg data files for "
                            + schemaName + "." + tableName, e);
        }
    }

    /**
     * Extracts per-file column statistics (min/max/null count) from manifest
     * bounds for the requested data file paths.
     */
    List<DataFileStatistics> getFileStatistics(String schemaName, String tableName,
                                               List<String> dataFilePaths) {
        try {
            TableIdentifier tableId = TableIdentifier.of(schemaName, tableName);
            Table table = catalog.loadTable(tableId);
            Schema schema = table.schema();
            Set<String> targetPaths = new HashSet<>(dataFilePaths);
            Snapshot snapshot = table.currentSnapshot();
            if (snapshot == null) {
                return List.of();
            }
            return scanFileStatistics(table, schema, targetPaths, snapshot);
        } catch (Exception e) {
            LOG.warn("action=get_file_statistics_failed schema={} table={} error={}",
                    schemaName, tableName, e.getMessage());
            return List.of();
        }
    }

    private List<DataFileStatistics> scanFileStatistics(Table table, Schema schema,
                                                        Set<String> targetPaths, Snapshot snapshot)
            throws java.io.IOException {
        List<DataFileStatistics> result = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile dataFile = task.file();
                String filePath = dataFile.path().toString();
                if (!targetPaths.contains(filePath)) {
                    continue;
                }
                long liveRowCount = IcebergFileStatisticsReader.liveRowCount(table, task);
                List<FileColumnStatistics> colStats =
                        extractColumnStatistics(dataFile, schema, liveRowCount);
                result.add(new DataFileStatistics(
                        filePath,
                        String.valueOf(snapshot.snapshotId()),
                        liveRowCount,
                        dataFile.fileSizeInBytes(),
                        colStats));
            }
        }
        return result;
    }

    private static List<FileColumnStatistics> extractColumnStatistics(
            DataFile dataFile, Schema schema, long liveRowCount) {
        Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
        Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();
        Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
        List<FileColumnStatistics> stats = new ArrayList<>();

        if (lowerBounds == null || upperBounds == null) {
            return stats;
        }

        for (Types.NestedField field : schema.columns()) {
            FileColumnStatistics columnStats = buildColumnStatistics(
                    field, lowerBounds, upperBounds, nullCounts, liveRowCount);
            if (columnStats != null) {
                stats.add(columnStats);
            }
        }
        return stats;
    }

    private static FileColumnStatistics buildColumnStatistics(
            Types.NestedField field,
            Map<Integer, ByteBuffer> lowerBounds,
            Map<Integer, ByteBuffer> upperBounds,
            Map<Integer, Long> nullCounts,
            long liveRowCount) {
        int fieldId = field.fieldId();
        ByteBuffer lowerBuf = lowerBounds.get(fieldId);
        ByteBuffer upperBuf = upperBounds.get(fieldId);
        if (lowerBuf == null || upperBuf == null) {
            return null;
        }
        Comparable<?> minVal = convertBound(field.type(), lowerBuf);
        Comparable<?> maxVal = convertBound(field.type(), upperBuf);
        if (minVal == null || maxVal == null) {
            return null;
        }
        long nullCount = nullCounts != null && nullCounts.containsKey(fieldId)
                ? nullCounts.get(fieldId) : 0L;
        return new FileColumnStatistics(
                field.name(), minVal, maxVal, nullCount,
                liveRowCount, 0L, 0L);
    }

    private static Comparable<?> convertBound(Type type, ByteBuffer buffer) {
        try {
            return (Comparable<?>) Conversions.fromByteBuffer(type, buffer);
        } catch (Exception e) {
            return null;
        }
    }
}
