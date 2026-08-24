package org.pangrova.connector.iceberg;

import org.pangrova.spi.index.statistics.FileColumnStatistics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds per-file column statistics for L0 partition pruning, sourcing values
 * from the connector's native (Iceberg) metadata rather than a JDBC sidecar:
 *
 * <ul>
 *   <li>min/max/nullCount come from {@link DataFile#lowerBounds()} / {@code upperBounds()} /
 *       {@code nullValueCounts()} (manifest stats).</li>
 *   <li>NDV is read from the table's Puffin {@link org.apache.iceberg.StatisticsFile}
 *       (Apache DataSketches Theta v1) when present — the Iceberg-standard format
 *       written by Spark/Trino {@code ANALYZE}. See {@link IcebergStatisticsCollector}
 *       for the table-level reader; this class scopes the same map per snapshot
 *       and looks up by Iceberg field ID.</li>
 *   <li>Live row count is {@code dataFile.recordCount()} minus the number of unique
 *       positions covered by the data file's delete files (positional deletes or
 *       v3 deletion-vector Puffin blobs). This makes cardinalities returned to L0
 *       pruning consistent with what {@code IcebergPageSourceProvider} actually
 *       emits for the file at scan time, addressing G-INDEX-07's deletion-vector
 *       awareness gap.</li>
 * </ul>
 *
 * <p>This reader replaces the connector-side data path of the legacy JDBC stats
 * sidecar ({@code JdbcIndexStatisticsStore}). The JDBC store remains as a cache
 * over Puffin (it is populated from connector results); plans-of-record are now
 * Puffin and live manifests.</p>
 */
final class IcebergFileStatisticsReader {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergFileStatisticsReader.class);

    private IcebergFileStatisticsReader() {}

    /**
     * Builds {@link FileColumnStatistics} for one Iceberg {@link FileScanTask},
     * applying deletion-vector awareness to the live row count and Puffin NDV
     * per column when available.
     *
     * @param table       Iceberg SDK table reference (used for Puffin / DV I/O)
     * @param task        scan task whose data file we report stats for
     * @param schema      table schema (for column ordering and bound decoding)
     * @param puffinNdv   field-ID → NDV from the snapshot's Puffin stats files;
     *                    pass an empty map if no Puffin stats are present
     * @return column statistics list, possibly empty if the data file has no
     *         lower/upper bounds in its manifest
     */
    static List<FileColumnStatistics> buildFileColumnStatistics(
            Table table, FileScanTask task, Schema schema, Map<Integer, Long> puffinNdv) {
        DataFile dataFile = task.file();
        long liveRowCount = liveRowCount(table, task);
        return extractColumnStatistics(dataFile, schema, puffinNdv, liveRowCount);
    }

    /**
     * Returns {@code dataFile.recordCount()} minus the count of unique deleted
     * positions covered by all delete files attached to {@code task}. Returns
     * the original record count if no delete files exist or if reading deletes
     * fails (best-effort — failures must not break planning).
     */
    static long liveRowCount(Table table, FileScanTask task) {
        long total = task.file().recordCount();
        List<DeleteFile> deletes = task.deletes();
        if (deletes == null || deletes.isEmpty()) {
            return total;
        }
        try {
            Set<Long> deleted = collectDeletedPositions(table, task);
            long live = total - deleted.size();
            return Math.max(live, 0L);
        } catch (RuntimeException e) {
            LOG.debug("event=dv_aware_count_failed file={} error={}",
                    task.file().path(), e.getMessage());
            return total;
        }
    }

    /**
     * Aggregates unique deleted row positions across all delete files associated
     * with the given scan task, including v3 Puffin deletion vectors and v2
     * positional delete Parquet files. Equality deletes are not counted as the
     * row count cost depends on the data — at planning time we conservatively
     * leave the row count unchanged for equality-only deletes.
     */
    private static Set<Long> collectDeletedPositions(Table table, FileScanTask task) {
        Set<Long> positions = new HashSet<>();
        String dataFilePath = task.file().path().toString();
        for (DeleteFile deleteFile : task.deletes()) {
            if (deleteFile.format() == FileFormat.PUFFIN) {
                readDvPositions(table, deleteFile, positions);
            } else if (deleteFile.content() == org.apache.iceberg.FileContent.POSITION_DELETES) {
                readPositionDeletePositions(table, deleteFile, dataFilePath, positions);
            }
            // EQUALITY_DELETES are skipped: they reference values, not positions,
            // so we cannot compute an exact deleted count without reading the data.
        }
        return positions;
    }

    private static void readDvPositions(
            Table table, DeleteFile dvFile, Set<Long> positions) {
        try (PuffinReader reader = Puffin
                .read(table.io().newInputFile(dvFile.path().toString()))
                .build()) {
            var blobs = reader.fileMetadata().blobs();
            if (blobs.isEmpty()) {
                return;
            }
            for (var blobMeta : blobs) {
                for (var pair : reader.readAll(List.of(blobMeta))) {
                    ByteBuffer blobData = pair.second();
                    byte[] bytes = toByteArray(blobData);
                    PositionDeleteIndex index =
                            PositionDeleteIndex.deserialize(bytes, dvFile);
                    index.forEach(positions::add);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read deletion vector for stats: " + dvFile.path(), e);
        }
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static final Schema POS_DELETE_SCHEMA = new Schema(
            Types.NestedField.required(2147483546, "file_path", Types.StringType.get()),
            Types.NestedField.required(2147483545, "pos", Types.LongType.get()));

    private static void readPositionDeletePositions(
            Table table, DeleteFile deleteFile, String dataFilePath, Set<Long> positions) {
        try (CloseableIterable<Record> reader = Parquet
                .read(table.io().newInputFile(deleteFile.path().toString()))
                .project(POS_DELETE_SCHEMA)
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(POS_DELETE_SCHEMA, fs))
                .build()) {
            for (Record record : reader) {
                Object pathObj = record.get(0, Object.class);
                Object posObj = record.get(1, Object.class);
                if (pathObj != null && posObj != null
                        && pathObj.toString().equals(dataFilePath)) {
                    positions.add(((Number) posObj).longValue());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read position delete file for stats: " + deleteFile.path(), e);
        }
    }

    private static List<FileColumnStatistics> extractColumnStatistics(
            DataFile dataFile, Schema schema,
            Map<Integer, Long> puffinNdv, long liveRowCount) {
        Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
        Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();
        Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
        List<FileColumnStatistics> stats = new ArrayList<>();

        for (Types.NestedField field : schema.columns()) {
            stats.add(buildColumnStatistics(
                    field, lowerBounds, upperBounds, nullCounts, puffinNdv, liveRowCount));
        }
        return stats;
    }

    private static FileColumnStatistics buildColumnStatistics(
            Types.NestedField field,
            Map<Integer, ByteBuffer> lowerBounds,
            Map<Integer, ByteBuffer> upperBounds,
            Map<Integer, Long> nullCounts,
            Map<Integer, Long> puffinNdv,
            long liveRowCount) {
        int fieldId = field.fieldId();
        Comparable<?> minVal = null;
        Comparable<?> maxVal = null;
        if (lowerBounds != null && upperBounds != null) {
            ByteBuffer lowerBuf = lowerBounds.get(fieldId);
            ByteBuffer upperBuf = upperBounds.get(fieldId);
            if (lowerBuf != null && upperBuf != null) {
                minVal = convertBound(field.type(), lowerBuf);
                maxVal = convertBound(field.type(), upperBuf);
            }
        }
        long nullCount = resolveNullCount(nullCounts, fieldId);
        long ndv = capNdv(puffinNdv, fieldId, liveRowCount);
        return new FileColumnStatistics(
                field.name(), minVal, maxVal, nullCount,
                liveRowCount, ndv, 0L);
    }

    private static long resolveNullCount(Map<Integer, Long> nullCounts, int fieldId) {
        return nullCounts != null && nullCounts.containsKey(fieldId)
                ? nullCounts.get(fieldId) : 0L;
    }

    private static long capNdv(Map<Integer, Long> puffinNdv, int fieldId, long liveRowCount) {
        long ndv = puffinNdv == null ? -1L : puffinNdv.getOrDefault(fieldId, -1L);
        if (ndv > liveRowCount && liveRowCount >= 0) {
            ndv = liveRowCount;
        }
        return ndv;
    }

    private static Comparable<?> convertBound(Type type, ByteBuffer buffer) {
        try {
            return (Comparable<?>) org.apache.iceberg.types.Conversions
                    .fromByteBuffer(type, buffer);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Convenience: returns an empty NDV map. Callers that don't yet have a
     * snapshot pinned can use this until {@link IcebergStatisticsCollector#readNdvFromPuffin}
     * is invoked.
     */
    static Map<Integer, Long> emptyPuffinNdv() {
        return new HashMap<>();
    }
}
