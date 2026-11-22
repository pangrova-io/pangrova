package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Collects column-level statistics from Iceberg manifest files and Puffin statistics files.
 * Reads NDV from Puffin Theta Sketch blobs when available (Iceberg 1.5+).
 */
final class IcebergStatisticsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergStatisticsCollector.class);

    /** Puffin blob property key for pre-computed NDV (written by Spark/Trino ANALYZE). */
    private static final String NDV_PROPERTY = "ndv";

    private IcebergStatisticsCollector() {}

    /**
     * Builds table statistics from an Iceberg snapshot summary and manifest files.
     */
    static TableStatistics buildStatistics(Snapshot snapshot, Table icebergTable, IcebergTableHandle handle) {
        return snapshotStatistics(snapshot, handle, () -> collectColumnStatistics(snapshot, icebergTable));
    }

    /** Binds query statistics using the selected metadata already loaded by the catalog. */
    static TableStatistics planningStatistics(
            Snapshot snapshot, Table table, IcebergTableHandle handle, Schema selectedSchema) {
        return snapshotStatistics(snapshot, handle, () -> {
            var ndv = readNdvFromPuffin(table, snapshot, false);
            Map<String, TableStatistics.ColumnStatistics> columns = new HashMap<>();
            for (Types.NestedField field : selectedSchema.columns()) {
                Long distinct = ndv.get(field.fieldId());
                columns.put(field.name(), distinct == null ? TableStatistics.ColumnStatistics.UNKNOWN
                        : new TableStatistics.ColumnStatistics(-1, distinct, null, null, -1.0));
            }
            return columns;
        });
    }

    private static TableStatistics snapshotStatistics(Snapshot snapshot, IcebergTableHandle handle,
            Supplier<Map<String, TableStatistics.ColumnStatistics>> columns) {
        // A range scan reads only files appended between its endpoints. The end
        // snapshot's live-file, row and byte totals describe a different source.
        if (IcebergTableHandle.READ_MODE_INCREMENTAL.equals(handle.readMode())) {
            return TableStatistics.UNKNOWN;
        }
        long rowCount = Long.parseLong(
                snapshot.summary().getOrDefault("total-records", "-1"));
        long dataSize = Long.parseLong(
                snapshot.summary().getOrDefault("total-files-size", "-1"));
        long sourceWorkUnits = Long.parseLong(
                snapshot.summary().getOrDefault("total-data-files", "-1"));
        return new TableStatistics(rowCount, dataSize, sourceWorkUnits, columns.get());
    }

    /**
     * Aggregates null counts, lower/upper bounds from all data files in manifests,
     * then builds per-column statistics.
     */
    static Map<String, TableStatistics.ColumnStatistics> collectColumnStatistics(
            Snapshot snapshot, Table icebergTable) {
        Schema schema = snapshot.schemaId() == null ? icebergTable.schema()
                : icebergTable.schemas().get(snapshot.schemaId());
        if (schema == null) throw new IllegalStateException("Selected statistics schema is unavailable");
        Map<String, TableStatistics.ColumnStatistics> colStats = new HashMap<>();
        for (Types.NestedField field : schema.columns()) {
            colStats.put(field.name(), TableStatistics.ColumnStatistics.UNKNOWN);
        }
        try {
            AggregatedManifestStats agg = aggregateFromManifests(snapshot, icebergTable, schema);
            Map<Integer, Long> puffinNdv = readNdvFromPuffin(icebergTable, snapshot, true);
            buildColumnStatistics(schema, agg, colStats, puffinNdv);
        } catch (Exception e) {
            LOG.debug("event=column_stats_collection_failed table={} error={}",
                    icebergTable.name(), e.getMessage());
        }
        return colStats;
    }

    /**
     * Scans all manifest files and aggregates null counts, bounds, file size, and file count.
     */
    private static AggregatedManifestStats aggregateFromManifests(
            Snapshot snapshot, Table icebergTable, Schema schema) throws Exception {
        Map<Integer, Long> nullCounts = new HashMap<>();
        Map<Integer, Comparable<?>> lowerBounds = new HashMap<>();
        Map<Integer, Comparable<?>> upperBounds = new HashMap<>();

        for (org.apache.iceberg.ManifestFile manifest : snapshot.dataManifests(icebergTable.io())) {
            try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DataFile> reader =
                         org.apache.iceberg.ManifestFiles.read(manifest, icebergTable.io())) {
                for (org.apache.iceberg.DataFile dataFile : reader) {
                    aggregateNullCounts(nullCounts, dataFile);
                    aggregateBounds(lowerBounds, upperBounds, dataFile, schema);
                }
            }
        }
        return new AggregatedManifestStats(nullCounts, lowerBounds, upperBounds);
    }

    /**
     * Converts aggregated manifest stats into per-column ColumnStatistics.
     * Reads NDV from Puffin statistics files when available.
     */
    private static void buildColumnStatistics(
            Schema schema, AggregatedManifestStats agg,
            Map<String, TableStatistics.ColumnStatistics> colStats,
            Map<Integer, Long> puffinNdv) {
        for (Types.NestedField field : schema.columns()) {
            int fieldId = field.fieldId();
            long nullCount = agg.nullCounts().getOrDefault(fieldId, -1L);
            Comparable<?> min = agg.lowerBounds().get(fieldId);
            Comparable<?> max = agg.upperBounds().get(fieldId);
            long ndv = puffinNdv.getOrDefault(fieldId, -1L);
            // File bytes divided by file/column count are not per-row value bytes.
            // The SDK metadata does not prove an uncompressed average value size.
            double avgSize = -1.0;
            colStats.put(field.name(),
                    new TableStatistics.ColumnStatistics(nullCount, ndv, min, max, avgSize));
        }
    }

    /**
     * Reads NDV estimates from Puffin statistics files attached to the table.
     * Scans all {@link StatisticsFile} entries for Theta Sketch blobs matching
     * the current snapshot, extracts NDV from blob properties or sketch data.
     *
     * @return field ID to NDV estimate; empty map if no Puffin stats available
     */
    private static Map<Integer, Long> readNdvFromPuffin(Table icebergTable, Snapshot snapshot, boolean readSketches) {
        List<StatisticsFile> statsFiles = icebergTable.statisticsFiles();
        if (statsFiles == null || statsFiles.isEmpty()) {
            return Map.of();
        }
        Map<Integer, Long> ndvByField = new HashMap<>();
        for (StatisticsFile sf : statsFiles) {
            if (sf.snapshotId() != snapshot.snapshotId()) {
                continue;
            }
            readNdvFromSingleFile(icebergTable, snapshot, sf, ndvByField, readSketches);
        }
        if (!ndvByField.isEmpty()) {
            LOG.debug("event=puffin_ndv_read table={} snapshotId={} columnsWithNdv={}",
                    icebergTable.name(), snapshot.snapshotId(), ndvByField.size());
        }
        return ndvByField;
    }

    private static void readNdvFromSingleFile(
            Table icebergTable, Snapshot snapshot, StatisticsFile sf, Map<Integer, Long> ndvByField,
            boolean readSketches) {
        for (BlobMetadata blob : sf.blobMetadata()) {
            if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())
                    || blob.sourceSnapshotId() != snapshot.snapshotId()
                    || blob.sourceSnapshotSequenceNumber() != snapshot.sequenceNumber()
                    || blob.fields().size() != 1) {
                continue;
            }
            long ndv = extractNdvFromBlobProperties(blob);
            if (ndv <= 0 && readSketches) ndv = extractNdvFromSketchBlob(icebergTable, sf, blob);
            if (ndv > 0) ndvByField.put(blob.fields().get(0), ndv);
        }
    }

    /**
     * Fast path: extract NDV from blob properties without reading the Puffin file.
     * Some writers (Spark, Trino) store the NDV estimate as a property.
     */
    private static long extractNdvFromBlobProperties(BlobMetadata blob) {
        Map<String, String> props = blob.properties();
        if (props == null) {
            return -1;
        }
        String ndvStr = props.get(NDV_PROPERTY);
        if (ndvStr == null) {
            return -1;
        }
        try {
            return Long.parseLong(ndvStr);
        } catch (NumberFormatException e) {
            LOG.debug("event=puffin_ndv_parse_failed value={}", ndvStr);
            return -1;
        }
    }

    /**
     * Slow path: read the Theta Sketch blob from the Puffin file and extract NDV.
     * Returns -1 if the sketch cannot be read or deserialized.
     */
    private static long extractNdvFromSketchBlob(
            Table icebergTable, StatisticsFile sf, BlobMetadata blob) {
        try (PuffinReader reader = Puffin.read(icebergTable.io().newInputFile(sf.path()))
                .build()) {
            org.apache.iceberg.puffin.FileMetadata fileMetadata = reader.fileMetadata();
            org.apache.iceberg.puffin.BlobMetadata matchingBlob = findMatchingBlob(
                    fileMetadata.blobs(), blob.fields());
            if (matchingBlob == null) {
                return -1;
            }
            for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> pair
                    : reader.readAll(List.of(matchingBlob))) {
                return estimateNdvFromThetaSketchBytes(pair.second());
            }
        } catch (Exception e) {
            LOG.debug("event=puffin_sketch_read_failed path={} error={}",
                    sf.path(), e.getMessage());
        }
        return -1;
    }

    private static org.apache.iceberg.puffin.BlobMetadata findMatchingBlob(
            List<org.apache.iceberg.puffin.BlobMetadata> puffinBlobs,
            List<Integer> targetFields) {
        for (org.apache.iceberg.puffin.BlobMetadata pb : puffinBlobs) {
            if (StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(pb.type())
                    && pb.inputFields().equals(targetFields)) {
                return pb;
            }
        }
        return null;
    }

    /**
     * Estimates NDV from a serialized Apache DataSketches Theta Sketch.
     * Parses the compact binary layout without requiring the DataSketches library.
     *
     * <p>Theta Sketch compact format (v3): byte 0 = preamble longs,
     * byte 5 = flags (bit 2 = empty). Estimate = retainedEntries / (theta / MAX_VALUE).</p>
     *
     * @return estimated NDV, or -1 if the sketch format is unrecognized
     */
    static long estimateNdvFromThetaSketchBytes(ByteBuffer sketchData) {
        if (sketchData == null || sketchData.remaining() < 16) {
            return -1;
        }
        try {
            ByteBuffer buf = sketchData.slice().order(java.nio.ByteOrder.LITTLE_ENDIAN);
            return decodeSketchEstimate(buf);
        } catch (Exception e) {
            LOG.debug("event=theta_sketch_decode_failed error={}", e.getMessage());
            return -1;
        }
    }

    private static long decodeSketchEstimate(ByteBuffer buf) {
        int preambleLongs = Byte.toUnsignedInt(buf.get(0));
        if (preambleLongs < 1) {
            return -1;
        }
        boolean isEmpty = (Byte.toUnsignedInt(buf.get(5)) & 0x04) != 0;
        if (isEmpty) {
            return 0;
        }
        if (preambleLongs == 1 && buf.remaining() == 16) {
            return 1;
        }
        return decodeRetainedEntries(buf, preambleLongs);
    }

    private static long decodeRetainedEntries(ByteBuffer buf, int preambleLongs) {
        int retainedEntries = buf.getInt(preambleLongs == 1 ? 4 : 8);
        if (retainedEntries <= 0) {
            return 0;
        }
        return estimateFromTheta(buf, preambleLongs, retainedEntries);
    }

    private static long estimateFromTheta(ByteBuffer buf, int preambleLongs, int retainedEntries) {
        int thetaOffset = Math.max(preambleLongs * 8 - 8, 8);
        long theta = buf.getLong(thetaOffset);
        if (theta == 0 || theta == Long.MAX_VALUE) {
            return retainedEntries;
        }
        double thetaFraction = (double) theta / Long.MAX_VALUE;
        return Math.round(retainedEntries / thetaFraction);
    }

    private static void aggregateNullCounts(Map<Integer, Long> nullCounts,
                                            org.apache.iceberg.DataFile dataFile) {
        Map<Integer, Long> fileNulls = dataFile.nullValueCounts();
        if (fileNulls == null) {
            return;
        }
        for (var entry : fileNulls.entrySet()) {
            nullCounts.merge(entry.getKey(), entry.getValue(), Math::addExact);
        }
    }

    private static void aggregateBounds(Map<Integer, Comparable<?>> lowerBounds,
                                        Map<Integer, Comparable<?>> upperBounds,
                                        org.apache.iceberg.DataFile dataFile,
                                        Schema schema) {
        mergeBoundsMap(lowerBounds, dataFile.lowerBounds(), schema, true);
        mergeBoundsMap(upperBounds, dataFile.upperBounds(), schema, false);
    }

    @SuppressWarnings("unchecked")
    private static void mergeBoundsMap(Map<Integer, Comparable<?>> target,
                                       Map<Integer, java.nio.ByteBuffer> raw,
                                       Schema schema, boolean keepSmaller) {
        if (raw == null) {
            return;
        }
        for (var entry : raw.entrySet()) {
            int fieldId = entry.getKey();
            Types.NestedField field = schema.findField(fieldId);
            if (field == null) {
                continue;
            }
            try {
                Comparable<Object> value = (Comparable<Object>) org.apache.iceberg.types.Conversions
                        .fromByteBuffer(field.type(), entry.getValue());
                target.merge(fieldId, value, (a, b) -> compareBounds(
                        (Comparable<Object>) a, (Comparable<Object>) b, keepSmaller));
            } catch (RuntimeException ignored) {
                // Retain existing bound if types are incomparable or conversion fails
            }
        }
    }

    private static Comparable<?> compareBounds(Comparable<Object> a, Comparable<Object> b,
                                                boolean keepSmaller) {
        try {
            int cmp = a.compareTo(b);
            return keepSmaller ? (cmp <= 0 ? a : b) : (cmp >= 0 ? a : b);
        } catch (ClassCastException e) {
            return a;
        }
    }

    /**
     * Intermediate aggregation result from manifest file scanning.
     */
    private record AggregatedManifestStats(
            Map<Integer, Long> nullCounts,
            Map<Integer, Comparable<?>> lowerBounds,
            Map<Integer, Comparable<?>> upperBounds) {}
}
