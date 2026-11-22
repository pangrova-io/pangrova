package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

/**
 * Executes Iceberg table compaction by rewriting data files that have
 * associated delete files, materializing deletion vectors (DVs) into
 * new clean data files.
 *
 * <p>For each data file with associated delete files (position deletes
 * or deletion vectors), this executor reads surviving rows, writes them
 * to a new Parquet file, and atomically replaces old data + delete files
 * via {@link RewriteFiles}.</p>
 */
public final class IcebergCompactionExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCompactionExecutor.class);

    private static final Schema POS_DELETE_SCHEMA = new Schema(
            Types.NestedField.required(2147483546, "file_path", Types.StringType.get()),
            Types.NestedField.required(2147483545, "pos", Types.LongType.get()));

    /** Default target size for merged data files. */
    public static final long DEFAULT_TARGET_FILE_SIZE_BYTES = 512L * 1024 * 1024;

    private static final long MAX_RECLUSTER_BYTES = 64L * 1024 * 1024;
    private static final long MAX_RECLUSTER_ROWS = 100_000L;
    private static final int MAX_RECLUSTER_FILES = 1_024;
    public static final String RECLUSTER_RUN = "pangrova.recluster.run-id";
    public static final String RECLUSTER_GENERATION = "pangrova.recluster.generation";

    private IcebergCompactionExecutor() {}

    /** Writes one disjoint set of replacement files without publishing a snapshot. */
    public static List<String> stageRecluster(Table table, SortOrder order, UUID attemptId,
            Consumer<List<Record>> arrange, Runnable requireActive) {
        Snapshot base = table.currentSnapshot();
        if (base == null) return List.of();
        var groups = planClustering(table, base);
        List<String> fragments = new ArrayList<>();
        for (List<FileScanTask> group : groups.values()) {
            List<Record> rows = new ArrayList<>();
            for (FileScanTask task : group) {
                requireActive.run();
                rows.addAll(readSurvivingRows(table, table.schema(), task, Set.of()));
            }
            if (!rows.isEmpty()) {
                requireActive.run();
                arrange.accept(rows);
                FileScanTask sample = group.get(0);
                writeDataFiles(table, table.schema(), sample.spec(), sample.file().partition(), rows,
                        DEFAULT_TARGET_FILE_SIZE_BYTES, order, "recluster-" + attemptId + "-")
                        .forEach(file -> fragments.add(ContentFileParser.toJson(file, table.spec())));
            }
        }
        requireActive.run();
        return List.copyOf(fragments);
    }

    /** Publishes only the replacement files produced for the winning attempt and exact base. */
    public static boolean publishRecluster(Table table, SortOrder order, UUID runId, String generation,
            UUID attemptId, List<String> fragments) {
        Snapshot base = table.currentSnapshot();
        var groups = base == null ? Map.<StructLikeWrapper, List<FileScanTask>>of() : planClustering(table, base);
        Set<DataFile> previous = new HashSet<>();
        Map<StructLikeWrapper, Long> remaining = new LinkedHashMap<>();
        var partitionType = table.spec().partitionType();
        groups.forEach((partition, tasks) -> {
            long rows = 0;
            for (FileScanTask task : tasks) {
                previous.add(task.file());
                rows = Math.addExact(rows, task.file().recordCount());
            }
            if (rows != 0) remaining.put(partition, rows);
        });
        if (fragments.size() > MAX_RECLUSTER_FILES) {
            throw new IllegalArgumentException("RECLUSTER received too many replacement files");
        }
        Set<DataFile> replacement = new HashSet<>();
        Set<String> paths = new HashSet<>();
        String prefix = table.location() + "/data/recluster-" + attemptId + "-";
        for (String fragment : fragments) {
            DataFile file = parseReclusterFile(fragment, table.spec(), order, prefix);
            if (!paths.add(file.path().toString())) {
                throw new IllegalArgumentException("RECLUSTER replacement repeats a data file");
            }
            var partition = StructLikeWrapper.forType(partitionType).set(file.partition());
            long rowsLeft = remaining.getOrDefault(partition, 0L) - file.recordCount();
            if (rowsLeft < 0) {
                throw new IllegalArgumentException("RECLUSTER replacement exceeds the admitted partition row count");
            }
            if (table.io().newInputFile(file.path().toString()).getLength() != file.fileSizeInBytes()) {
                throw new IllegalArgumentException("RECLUSTER replacement size differs from the staged object");
            }
            remaining.put(partition, rowsLeft);
            replacement.add(file);
        }
        if (remaining.values().stream().anyMatch(rows -> rows != 0)) {
            throw new IllegalArgumentException("RECLUSTER replacement does not cover the admitted files");
        }
        if (previous.isEmpty()) return false;
        table.newRewrite()
                .validateFromSnapshot(base.snapshotId())
                .set(RECLUSTER_RUN, runId.toString())
                .set(RECLUSTER_GENERATION, generation)
                .rewriteFiles(previous, replacement)
                .commit();
        return true;
    }

    private static DataFile parseReclusterFile(String fragment, PartitionSpec spec, SortOrder order, String prefix) {
        var content = JsonUtil.parse(fragment, node -> ContentFileParser.fromJson(node, spec));
        if (!(content instanceof DataFile file)) {
            throw new IllegalArgumentException("RECLUSTER requires data-file fragments");
        }
        String path = file.path().toString();
        if (!path.startsWith(prefix) || !path.substring(prefix.length()).matches("[0-9a-f-]{36}\\.parquet")
                || file.format() != FileFormat.PARQUET || file.specId() != spec.specId()
                || !Integer.valueOf(order.orderId()).equals(file.sortOrderId())
                || file.recordCount() <= 0 || file.fileSizeInBytes() <= 0) {
            throw new IllegalArgumentException("RECLUSTER replacement differs from the winning attempt");
        }
        return file;
    }

    private static Map<StructLikeWrapper, List<FileScanTask>> planClustering(Table table, Snapshot snapshot) {
        Map<StructLikeWrapper, List<FileScanTask>> groups = new LinkedHashMap<>();
        long bytes = 0;
        long rows = 0;
        int files = 0;
        try (var tasks = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.spec().specId() != table.spec().specId()
                        || task.file().format() != FileFormat.PARQUET
                        || !task.deletes().isEmpty()) {
                    throw new UnsupportedOperationException(
                            "RECLUSTER requires Parquet files with the current partition spec and no row deletes");
                }
                bytes = Math.addExact(bytes, task.file().fileSizeInBytes());
                rows = Math.addExact(rows, task.file().recordCount());
                if (++files > MAX_RECLUSTER_FILES || bytes > MAX_RECLUSTER_BYTES || rows > MAX_RECLUSTER_ROWS) {
                    throw new IllegalArgumentException(
                            "RECLUSTER input exceeds the bounded connector rewrite limit");
                }
                var partition = StructLikeWrapper.forType(table.spec().partitionType()).set(task.file().partition());
                groups.computeIfAbsent(partition, ignored -> new ArrayList<>()).add(task);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to plan Iceberg clustering rewrite", failure);
        }
        return groups;
    }

    /**
     * Compacts the table using Iceberg's bin-pack planner: files carrying deletes
     * are materialized and small files are merged into target-sized outputs.
     *
     * @param table the Iceberg table to compact
     * @return summary with file counts and bytes rewritten
     */
    public static CompactionSummary compact(Table table) {
        return compact(table, DEFAULT_TARGET_FILE_SIZE_BYTES);
    }

    /**
     * Compacts the table with an explicit target output file size.
     *
     * <p>{@code delete-file-threshold=1} is mandatory: the planner default is
     * {@link Integer#MAX_VALUE}, which would silently stop rewriting
     * delete-carrying files and regress the previous behavior.</p>
     *
     * @param table                 the Iceberg table to compact
     * @param targetFileSizeBytes   soft target for each produced data file
     * @return summary with file counts and bytes rewritten
     */
    public static CompactionSummary compact(Table table, long targetFileSizeBytes) {
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive");
        }
        table.refresh();
        var baseSnapshot = table.currentSnapshot();
        if (baseSnapshot == null) {
            return new CompactionSummary(0, 0, 0L, 0L, 0);
        }

        var planner = new BinPackRewriteFilePlanner(table);
        planner.init(Map.of(
                BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, "1"));
        List<RewriteFileGroup> groups = new ArrayList<>();
        try (CloseableIterable<RewriteFileGroup> planned = planner.plan().groups()) {
            for (RewriteFileGroup group : planned) {
                groups.add(group);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to plan compaction groups", e);
        }
        if (groups.isEmpty()) {
            LOG.info("event=compact_skip table={} reason=no_rewrite_groups", table.name());
            return new CompactionSummary(0, 0, 0L, 0L, 0);
        }

        LOG.info("event=compact_start table={} groups={} targetFileSizeBytes={}",
                table.name(), groups.size(), targetFileSizeBytes);

        Set<DataFile> oldDataFiles = new HashSet<>();
        Set<DeleteFile> oldDeleteFiles = new HashSet<>();
        Set<DataFile> newDataFiles = new HashSet<>();
        long totalBytesRewritten = 0L;

        for (RewriteFileGroup group : groups) {
            boolean mergeable = isMergeableGroup(table, group);
            if (mergeable) {
                rewriteGroup(table, group, targetFileSizeBytes,
                        oldDataFiles, oldDeleteFiles, newDataFiles);
            } else {
                // Mixed partition specs cannot share one output file without the
                // per-row PartitionKey rewrite; keep the strict per-file path.
                for (FileScanTask task : group.fileScanTasks()) {
                    DataFile newFile = rewriteTask(table, task);
                    oldDataFiles.add(task.file());
                    task.deletes().forEach(oldDeleteFiles::add);
                    if (newFile != null) {
                        newDataFiles.add(newFile);
                    }
                }
            }
            for (FileScanTask task : group.fileScanTasks()) {
                totalBytesRewritten += task.file().fileSizeInBytes();
            }
        }

        commitRewrite(table, baseSnapshot.snapshotId(), oldDataFiles, oldDeleteFiles, newDataFiles);

        long totalBytesProduced = newDataFiles.stream()
                .mapToLong(DataFile::fileSizeInBytes)
                .sum();
        long bytesSaved = totalBytesRewritten - totalBytesProduced;

        LOG.info("event=compact_done table={} filesCompacted={} filesProduced={} "
                        + "bytesRewritten={} bytesSaved={} deleteFilesRemoved={}",
                table.name(), oldDataFiles.size(), newDataFiles.size(),
                totalBytesRewritten, bytesSaved, oldDeleteFiles.size());

        return new CompactionSummary(
                oldDataFiles.size(), newDataFiles.size(),
                totalBytesRewritten, bytesSaved, oldDeleteFiles.size());
    }

    private static boolean isMergeableGroup(Table table, RewriteFileGroup group) {
        List<FileScanTask> tasks = group.fileScanTasks();
        if (tasks.isEmpty()) {
            return false;
        }
        int specId = group.outputSpecId();
        Object partition = tasks.get(0).file().partition();
        for (FileScanTask task : tasks) {
            if (task.spec().specId() != specId) {
                return false;
            }
            if (!task.file().partition().equals(partition)) {
                return false;
            }
        }
        return true;
    }

    private static void rewriteGroup(Table table, RewriteFileGroup group,
                                     long targetFileSizeBytes,
                                     Set<DataFile> oldDataFiles,
                                     Set<DeleteFile> oldDeleteFiles,
                                     Set<DataFile> newDataFiles) {
        Schema schema = table.schema();
        List<Record> rows = new ArrayList<>();
        for (FileScanTask task : group.fileScanTasks()) {
            rejectEqualityDeletes(task);
            Set<Long> deletedPositions = collectDeletedPositions(table, task);
            rows.addAll(readSurvivingRows(table, schema, task, deletedPositions));
            oldDataFiles.add(task.file());
            task.deletes().forEach(oldDeleteFiles::add);
        }
        if (rows.isEmpty()) {
            return;
        }
        sortBySortOrder(rows, table.sortOrder(), schema);
        FileScanTask sample = group.fileScanTasks().get(0);
        newDataFiles.addAll(writeDataFiles(table, schema, sample.spec(),
                sample.file().partition(), rows, targetFileSizeBytes));
    }

    private static void rejectEqualityDeletes(FileScanTask task) {
        for (DeleteFile deleteFile : task.deletes()) {
            if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                throw new UnsupportedOperationException(
                        "Compaction cannot materialize equality deletes without a "
                                + "row-level key match: " + deleteFile.path());
            }
        }
    }

    private static DataFile rewriteTask(Table table, FileScanTask task) {
        Schema schema = table.schema();
        rejectEqualityDeletes(task);
        Set<Long> deletedPositions = collectDeletedPositions(table, task);
        List<Record> surviving = readSurvivingRows(table, schema, task, deletedPositions);

        if (surviving.isEmpty()) {
            return null;
        }

        sortBySortOrder(surviving, table.sortOrder(), schema);
        return writeDataFile(table, schema, task.spec(), task.file().partition(), surviving);
    }

    /**
     * Sorts records according to the table's sort order. If the table has no
     * sort order defined (unsorted), records are written in their original order.
     */
    public static void sortBySortOrder(List<Record> records,
                                        org.apache.iceberg.SortOrder sortOrder,
                                        Schema schema) {
        if (sortOrder == null || sortOrder.isUnsorted() || records.size() <= 1) {
            return;
        }
        java.util.Comparator<Record> comparator = buildSortOrderComparator(sortOrder, schema);
        records.sort(comparator);
    }

    private static java.util.Comparator<Record> buildSortOrderComparator(
            org.apache.iceberg.SortOrder sortOrder, Schema schema) {
        java.util.Comparator<Record> comparator = null;
        for (org.apache.iceberg.SortField sortField : sortOrder.fields()) {
            int sourceId = sortField.sourceId();
            Types.NestedField field = schema.findField(sourceId);
            if (field == null) {
                continue;
            }
            String fieldName = field.name();
            boolean ascending = sortField.direction() == org.apache.iceberg.SortDirection.ASC;
            java.util.Comparator<Record> fieldComparator = (r1, r2) -> {
                Object v1 = r1.getField(fieldName);
                Object v2 = r2.getField(fieldName);
                return compareValues(v1, v2);
            };
            if (!ascending) {
                fieldComparator = fieldComparator.reversed();
            }
            comparator = (comparator == null) ? fieldComparator : comparator.thenComparing(fieldComparator);
        }
        return comparator != null ? comparator : java.util.Comparator.comparingInt(r -> 0);
    }

    @SuppressWarnings("unchecked")
    private static int compareValues(Object v1, Object v2) {
        if (v1 == null && v2 == null) {
            return 0;
        }
        if (v1 == null) {
            return 1; // nulls last
        }
        if (v2 == null) {
            return -1; // nulls last
        }
        if (v1 instanceof Comparable<?> c1) {
            return ((Comparable<Object>) c1).compareTo(v2);
        }
        return 0;
    }

    static Set<Long> collectDeletedPositions(Table table, FileScanTask task) {
        Set<Long> positions = new HashSet<>();
        String dataFilePath = task.file().path().toString();

        for (DeleteFile deleteFile : task.deletes()) {
            if (deleteFile.format() == FileFormat.PUFFIN) {
                readDvPositions(table, deleteFile, positions);
            } else {
                readPositionDeletePositions(table, deleteFile, dataFilePath, positions);
            }
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
                    index.forEach(pos -> positions.add(pos));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read deletion vector: " + dvFile.path(), e);
        }
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static void readPositionDeletePositions(
            Table table, DeleteFile deleteFile, String dataFilePath,
            Set<Long> positions) {
        try (CloseableIterable<Record> reader = Parquet
                .read(table.io().newInputFile(deleteFile.path().toString()))
                .project(POS_DELETE_SCHEMA)
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(
                        POS_DELETE_SCHEMA, fs))
                .build()) {

            for (Record record : reader) {
                // Position delete records: field 0 = file_path, field 1 = pos
                Object pathObj = record.get(0, Object.class);
                Object posObj = record.get(1, Object.class);
                if (pathObj != null && posObj != null
                        && pathObj.toString().equals(dataFilePath)) {
                    positions.add(((Number) posObj).longValue());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read position delete file: " + deleteFile.path(), e);
        }
    }

    private static List<Record> readSurvivingRows(
            Table table, Schema schema, FileScanTask task,
            Set<Long> deletedPositions) {
        List<Record> surviving = new ArrayList<>();
        forEachSurvivingRow(table, schema, task, deletedPositions, (row, position) -> surviving.add(row));
        return surviving;
    }

    static void forEachSurvivingRow(Table table, Schema schema, FileScanTask task,
            Set<Long> deletedPositions, ObjLongConsumer<Record> consumer) {
        try (CloseableIterable<Record> reader = Parquet
                .read(table.io().newInputFile(task.file().path().toString()))
                .project(schema)
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(schema, fs))
                .split(task.start(), task.length())
                .build()) {

            long position = task.start();
            for (Record row : reader) {
                if (!deletedPositions.contains(position)) {
                    consumer.accept(row, position);
                }
                position++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read data file: " + task.file().path(), e);
        }
    }

    private static DataFile writeDataFile(
            Table table, Schema schema, PartitionSpec spec,
            org.apache.iceberg.StructLike partition, List<Record> rows) {
        return writeDataFiles(table, schema, spec, partition, rows,
                Long.MAX_VALUE).get(0);
    }

    /** Streams rows into target-sized data files without retaining a second copy. */
    private static List<DataFile> writeDataFiles(
            Table table, Schema schema, PartitionSpec spec,
            org.apache.iceberg.StructLike partition, List<Record> rows,
            long targetFileSizeBytes) {
        return writeDataFiles(table, schema, spec, partition, rows, targetFileSizeBytes, SortOrder.unsorted(), "");
    }

    private static List<DataFile> writeDataFiles(
            Table table, Schema schema, PartitionSpec spec,
            org.apache.iceberg.StructLike partition, List<Record> rows,
            long targetFileSizeBytes, SortOrder order, String filePrefix) {
        List<DataFile> files = new ArrayList<>();
        DataWriter<Record> writer = null;
        try {
            for (Record row : rows) {
                if (writer == null) {
                    writer = newWriter(table, schema, spec, partition, order, filePrefix);
                }
                writer.write(row);
                if (writer.length() >= targetFileSizeBytes) {
                    writer.close();
                    files.add(writer.toDataFile());
                    writer = null;
                }
            }
            if (writer != null) {
                writer.close();
                files.add(writer.toDataFile());
            }
            return files;
        } catch (IOException e) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw new UncheckedIOException("Failed to write compacted data file", e);
        }
    }

    private static DataWriter<Record> newWriter(Table table, Schema schema,
            PartitionSpec spec, org.apache.iceberg.StructLike partition, SortOrder order, String filePrefix)
            throws IOException {
        String path = table.location() + "/data/" + filePrefix + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(path);
        return Parquet.writeData(outputFile)
                .schema(schema)
                .withSpec(spec)
                .withPartition(partition)
                .withSortOrder(order)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build();
    }

    private static void commitRewrite(
            Table table,
            long baseSnapshotId,
            Set<DataFile> oldDataFiles,
            Set<DeleteFile> oldDeleteFiles,
            Set<DataFile> newDataFiles) {
        RewriteFiles rewrite = table.newRewrite();
        rewrite.validateFromSnapshot(baseSnapshotId);
        rewrite.rewriteFiles(oldDataFiles, oldDeleteFiles, newDataFiles, Set.of());
        rewrite.commit();
    }

    /**
     * Summary of a compaction operation.
     *
     * @param filesCompacted     number of data files rewritten (had associated deletes)
     * @param filesProduced      number of new clean data files written
     * @param bytesRewritten     total bytes of original data files that were rewritten
     * @param bytesSaved         bytes saved (rewritten minus produced); negative if new files are larger
     * @param deleteFilesRemoved number of delete files (DVs + position deletes) removed
     */
    public record CompactionSummary(
            int filesCompacted,
            int filesProduced,
            long bytesRewritten,
            long bytesSaved,
            int deleteFilesRemoved
    ) {}
}
