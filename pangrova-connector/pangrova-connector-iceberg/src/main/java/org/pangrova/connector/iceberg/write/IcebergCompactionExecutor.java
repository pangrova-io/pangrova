package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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
import java.util.List;
import java.util.Set;
import java.util.UUID;

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

    private IcebergCompactionExecutor() {}

    /**
     * Compacts the table by rewriting data files that have associated
     * delete files. Data files without deletes are left untouched.
     *
     * @param table the Iceberg table to compact
     * @return summary with file counts and bytes rewritten
     */
    public static CompactionSummary compact(Table table) {
        table.refresh();

        List<FileScanTask> tasksWithDeletes = collectTasksWithDeletes(table);
        if (tasksWithDeletes.isEmpty()) {
            LOG.info("event=compact_skip table={} reason=no_delete_files", table.name());
            return new CompactionSummary(0, 0, 0L, 0L, 0);
        }

        LOG.info("event=compact_start table={} tasksWithDeletes={}",
                table.name(), tasksWithDeletes.size());

        Set<DataFile> oldDataFiles = new HashSet<>();
        Set<DeleteFile> oldDeleteFiles = new HashSet<>();
        Set<DataFile> newDataFiles = new HashSet<>();
        long totalBytesRewritten = 0L;

        for (FileScanTask task : tasksWithDeletes) {
            oldDataFiles.add(task.file());
            task.deletes().forEach(oldDeleteFiles::add);

            DataFile newFile = rewriteTask(table, task);
            if (newFile != null) {
                newDataFiles.add(newFile);
            }
            totalBytesRewritten += task.file().fileSizeInBytes();
        }

        commitRewrite(table, oldDataFiles, oldDeleteFiles, newDataFiles);

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

    private static List<FileScanTask> collectTasksWithDeletes(Table table) {
        List<FileScanTask> result = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()) {
                    result.add(task);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to plan scan for compaction", e);
        }
        return result;
    }

    private static DataFile rewriteTask(Table table, FileScanTask task) {
        Schema schema = table.schema();
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
    private static void sortBySortOrder(List<Record> records,
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

    private static Set<Long> collectDeletedPositions(Table table, FileScanTask task) {
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

        try (CloseableIterable<Record> reader = Parquet
                .read(table.io().newInputFile(task.file().path().toString()))
                .project(schema)
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(schema, fs))
                .split(task.start(), task.length())
                .build()) {

            long position = 0;
            for (Record row : reader) {
                if (!deletedPositions.contains(position)) {
                    surviving.add(row);
                }
                position++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read data file: " + task.file().path(), e);
        }
        return surviving;
    }

    private static DataFile writeDataFile(
            Table table, Schema schema, PartitionSpec spec,
            org.apache.iceberg.StructLike partition, List<Record> rows) {
        String path = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(path);

        try {
            DataWriter<Record> writer = Parquet.writeData(outputFile)
                    .schema(schema)
                    .withSpec(spec)
                    .withPartition(partition)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .build();

            for (Record row : rows) {
                writer.write(row);
            }
            writer.close();
            return writer.toDataFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write compacted data file", e);
        }
    }

    private static void commitRewrite(
            Table table,
            Set<DataFile> oldDataFiles,
            Set<DeleteFile> oldDeleteFiles,
            Set<DataFile> newDataFiles) {
        RewriteFiles rewrite = table.newRewrite();
        rewrite.validateFromSnapshot(table.currentSnapshot().snapshotId());
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
