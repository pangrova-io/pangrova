package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Builds Iceberg delete files (position deletes, equality deletes, and
 * deletion vectors). Returns {@link DeleteFile} descriptors without
 * committing; the caller commits via
 * {@code IcebergMetadata.finishDistributedWrite()}.
 */
public final class IcebergDeleteWriter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergDeleteWriter.class);

    private IcebergDeleteWriter() {}

    /**
     * Builds a position delete file without committing.
     * Caller is responsible for committing via {@code finishDistributedWrite()}.
     *
     * @param table        target Iceberg table
     * @param dataFilePath path of the data file whose rows are being deleted
     * @param positions    sorted row positions to delete
     * @return the built DeleteFile descriptor
     */
    public static DeleteFile buildPositionDeleteFile(
            Table table, String dataFilePath, List<Long> positions) {
        OutputFile outputFile = newDeleteOutputFile(table);
        PositionDeleteWriter<Record> writer = createPositionWriter(outputFile, table);
        try {
            writePositions(writer, table.schema(), dataFilePath, positions);
            writer.close();
            return writer.toDeleteFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write position delete file", e);
        }
    }

    private static PositionDeleteWriter<Record> createPositionWriter(
            OutputFile outputFile, Table table) {
        try {
            return Parquet.writeDeletes(outputFile)
                    .forTable(table)
                    .withSpec(PartitionSpec.unpartitioned())
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .buildPositionWriter();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create position delete writer", e);
        }
    }

    private static void writePositions(
            PositionDeleteWriter<Record> writer,
            Schema schema,
            String dataFilePath,
            List<Long> positions) {
        // Iceberg 1.4.x requires non-null row for GenericParquetWriter
        Record emptyRow = GenericRecord.create(schema);
        PositionDelete<Record> posDelete = PositionDelete.create();
        for (long pos : positions) {
            writer.write(posDelete.set(dataFilePath, pos, emptyRow));
        }
    }

    // -- deletion vector (v3) --

    /**
     * Builds a deletion vector (DV) Puffin file without committing.
     *
     * <p>Uses Iceberg 1.10.x {@link BaseDVFileWriter} to write a
     * RoaringBitmap of deleted row positions into a Puffin blob.
     * The returned {@link DeleteFile} descriptors carry the Puffin
     * file path and content type for coordinator-side commit.
     *
     * @param table        target Iceberg v3 table
     * @param dataFilePath path of the data file whose rows are being deleted
     * @param positions    sorted row positions to delete (must not be empty)
     * @return list of DeleteFile descriptors (one per data file referenced)
     * @throws IllegalArgumentException if positions is empty
     */
    public static List<DeleteFile> buildDeletionVectorFile(
            Table table, String dataFilePath, List<Long> positions) {
        if (positions.isEmpty()) {
            throw new IllegalArgumentException("positions must not be empty");
        }

        OutputFileFactory fileFactory = OutputFileFactory
                .builderFor(table, 0, 0)
                .format(FileFormat.PUFFIN)
                .build();

        try (DVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, path -> null)) {
            PartitionSpec spec = table.spec();
            for (long pos : positions) {
                dvWriter.delete(dataFilePath, pos, spec, null);
            }
            dvWriter.close();
            DeleteWriteResult result = dvWriter.result();
            return result.deleteFiles();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write deletion vector file", e);
        }
    }

    /**
     * Merges multiple sets of delete positions for the same data file into a
     * single deletion vector Puffin file.
     *
     * <p>In Iceberg v3, each data file has at most one active DV. When multiple
     * workers produce DV fragments for the same data file, or when a second
     * DELETE targets a data file that already has a DV, the positions must be
     * merged (OR'd) into a single bitmap.
     *
     * <p>This method combines all position sets into one {@link BaseDVFileWriter}
     * session, which accumulates them into a single RoaringBitmap per data file.
     * The caller is responsible for removing old DV files and committing the
     * new one via {@code RowDelta}.
     *
     * @param table           target Iceberg v3 table
     * @param dataFilePath    path of the data file being deleted from
     * @param positionSets    two or more sets of row positions to merge (each non-empty)
     * @return list of DeleteFile descriptors for the merged DV
     * @throws IllegalArgumentException if positionSets is empty or contains empty sets
     */
    public static List<DeleteFile> mergeDeletionVectors(
            Table table,
            String dataFilePath,
            List<List<Long>> positionSets) {
        if (positionSets.isEmpty()) {
            throw new IllegalArgumentException("positionSets must not be empty");
        }

        List<Long> allPositions = new ArrayList<>();
        for (List<Long> positions : positionSets) {
            if (positions.isEmpty()) {
                throw new IllegalArgumentException(
                        "Each position set must be non-empty");
            }
            allPositions.addAll(positions);
        }

        LOG.info("event=merge_deletion_vectors dataFile={} fragmentCount={} "
                        + "totalPositions={}",
                dataFilePath, positionSets.size(), allPositions.size());

        // BaseDVFileWriter accumulates all positions into a single RoaringBitmap
        // per data file, handling deduplication via OR semantics automatically.
        return buildDeletionVectorFile(table, dataFilePath, allPositions);
    }

    // -- equality delete internals --

    /**
     * Builds an equality delete file without committing.
     * Caller is responsible for committing via {@code finishDistributedWrite()}.
     *
     * @param table            target Iceberg table
     * @param equalityFieldIds field IDs that form the equality key
     * @param deleteSchema     schema containing only the equality columns
     * @param deletedRecords   records whose equality key values identify rows to delete
     * @return the built DeleteFile descriptor
     */
    public static DeleteFile buildEqualityDeleteFile(
            Table table,
            int[] equalityFieldIds,
            Schema deleteSchema,
            List<Record> deletedRecords) {
        OutputFile outputFile = newDeleteOutputFile(table);
        EqualityDeleteWriter<Record> writer =
                createEqualityWriter(outputFile, table, equalityFieldIds, deleteSchema);
        try {
            deletedRecords.forEach(writer::write);
            writer.close();
            return writer.toDeleteFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write equality delete file", e);
        }
    }

    private static EqualityDeleteWriter<Record> createEqualityWriter(
            OutputFile outputFile,
            Table table,
            int[] equalityFieldIds,
            Schema deleteSchema) {
        try {
            return Parquet.writeDeletes(outputFile)
                    .forTable(table)
                    .withSpec(PartitionSpec.unpartitioned())
                    .createWriterFunc(GenericParquetWriter::create)
                    .equalityFieldIds(equalityFieldIds)
                    .rowSchema(deleteSchema)
                    .overwrite()
                    .buildEqualityWriter();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create equality delete writer", e);
        }
    }

    // -- shared helpers --

    private static OutputFile newDeleteOutputFile(Table table) {
        String path = table.location() + "/data/" + UUID.randomUUID() + "-deletes.parquet";
        return table.io().newOutputFile(path);
    }
}
