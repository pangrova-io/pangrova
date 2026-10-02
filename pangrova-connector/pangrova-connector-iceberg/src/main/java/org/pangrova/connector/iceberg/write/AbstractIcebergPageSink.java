package org.pangrova.connector.iceberg.write;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.protocol.batch.ColumnarPageSink;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.page.NativePageConverter;
import org.pangrova.spi.connector.data.page.Page;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Base class for Iceberg page sinks that handles row conversion, value
 * coercion, and partition-aware file writing.
 *
 * <p>For unpartitioned tables, rows are written directly to a single data
 * file. For partitioned tables, rows are buffered in memory and grouped
 * by partition key at commit time, producing one data file per partition.
 *
 * <p>Subclasses provide the metadata collection strategy via {@link #collectCommitMetadata()}.
 *
 * <p>Not thread-safe: instances are confined to a single writer thread.
 */
@NotThreadSafe
abstract class AbstractIcebergPageSink implements ColumnarPageSink {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractIcebergPageSink.class);

    static final int MAX_OPEN_WRITERS = 128;

    protected final Table table;
    protected final Schema writeSchema;
    private final Schema parquetSafeSchema;
    protected final boolean partitioned;
    protected final String schemaName;
    protected final String tableName;
    private final Optional<Map<String, String>> encryptionProperties;

    /** Writer for unpartitioned tables; null when partitioned. */
    private final DataWriter<Record> writer;
    /** Per-partition writers for streaming partitioned writes; null when unpartitioned. */
    private final Map<String, DataWriter<Record>> partitionWriters;
    /** DataFiles from partition writers closed early due to MAX_OPEN_WRITERS limit. */
    private final List<DataFile> closedPartitionFiles;
    /** Partition key reused for routing; null when unpartitioned. */
    private final PartitionKey partitionKey;
    /** Adapter for partition key evaluation; null when unpartitioned. */
    private final PartitionStructProjection partitionAdapter;

    private boolean finished;
    private boolean aborted;

    protected AbstractIcebergPageSink(IcebergWriteContext ctx) {
        this.table = ctx.table();
        this.writeSchema = ctx.writeSchema();
        this.parquetSafeSchema = IcebergValueCoercion.sanitizeSchemaForParquet(writeSchema);
        this.schemaName = ctx.schemaName();
        this.tableName = ctx.tableName();
        this.encryptionProperties = ctx.encryptionProperties();
        this.partitioned = table.spec().isPartitioned();
        this.writer = partitioned ? null : initUnpartitionedWriter();
        if (partitioned) {
            this.partitionWriters = new LinkedHashMap<>();
            this.closedPartitionFiles = new ArrayList<>();
            this.partitionKey = new PartitionKey(table.spec(), writeSchema);
            this.partitionAdapter = new PartitionStructProjection(writeSchema);
        } else {
            this.partitionWriters = null;
            this.closedPartitionFiles = new ArrayList<>();
            this.partitionKey = null;
            this.partitionAdapter = null;
        }
    }

    @Override
    public void appendPage(Page page) {
        for (TypedRow row : page.rows()) {
            writeRecord(convertRow(row));
        }
    }

    /**
     * Converts and writes each row synchronously without retaining the caller's batch.
     *
     * @param batch caller-owned batch, with missing target columns written as null
     */
    @Override
    public void appendColumnarBatch(ColumnarBatch batch) {
        List<Types.NestedField> fields = writeSchema.columns();
        List<String> columns = batch.columns();
        int[] ordinals = new int[fields.size()];
        for (int column = 0; column < fields.size(); column++) {
            ordinals[column] = columns.indexOf(fields.get(column).name());
        }
        int rowCount = batch.rowCount();
        for (int row = 0; row < rowCount; row++) {
            GenericRecord record = GenericRecord.create(parquetSafeSchema);
            for (int column = 0; column < fields.size(); column++) {
                Object value = ordinals[column] < 0 ? null : batch.value(row, ordinals[column]);
                record.set(column, IcebergValueCoercion.coerceValue(value, fields.get(column).type()));
            }
            writeRecord(record);
        }
    }

    @Override
    public void appendNativePage(NativePage page) {
        appendPage(NativePageConverter.toPage(page));
    }

    private void writeRecord(GenericRecord record) {
        if (partitioned) {
            writeToPartition(record);
        } else {
            writer.write(record);
        }
    }

    private void writeToPartition(GenericRecord record) {
        partitionAdapter.wrap(record);
        partitionKey.partition(partitionAdapter);
        String path = partitionKey.toPath();
        DataWriter<Record> pw = partitionWriters.get(path);
        if (pw == null) {
            if (partitionWriters.size() >= MAX_OPEN_WRITERS) {
                evictLruPartitionWriter();
            }
            // Create a dedicated key for this partition writer
            PartitionKey dedicatedKey = new PartitionKey(table.spec(), writeSchema);
            dedicatedKey.partition(partitionAdapter);
            pw = initPartitionWriter(dedicatedKey);
            partitionWriters.put(path, pw);
        }
        pw.write(record);
    }

    private void evictLruPartitionWriter() {
        var iterator = partitionWriters.entrySet().iterator();
        if (!iterator.hasNext()) {
            return;
        }
        var lruEntry = iterator.next();
        iterator.remove();
        try {
            lruEntry.getValue().close();
            closedPartitionFiles.add(lruEntry.getValue().toDataFile());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close evicted partition writer", e);
        }
    }

    private DataWriter<Record> initPartitionWriter(PartitionKey key) {
        PartitionSpec spec = table.spec();
        String filePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filePath);
        try {
            var builder = Parquet.writeData(outputFile)
                    .schema(parquetSafeSchema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(spec)
                    .withPartition(key);
            applyEncryptionProperties(builder);
            return builder.build();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize partition writer", e);
        }
    }

    @Override
    public byte[] finishWithMetadata() {
        if (finished || aborted) {
            return new byte[0];
        }
        finished = true;
        return collectCommitMetadata();
    }

    @Override
    public void abort() {
        if (finished || aborted) {
            return;
        }
        aborted = true;
        deletePartialFiles();
    }

    /**
     * Deletes partial data files written by this sink that have not been committed.
     * Called during abort to prevent orphan files from accumulating in storage.
     */
    private void deletePartialFiles() {
        if (!partitioned && writer != null) {
            deleteWriterFile(writer);
        }
        if (partitionWriters != null) {
            for (DataWriter<Record> pw : partitionWriters.values()) {
                deleteWriterFile(pw);
            }
        }
    }

    private void deleteWriterFile(DataWriter<Record> dataWriter) {
        try {
            dataWriter.close();
            DataFile dataFile = dataWriter.toDataFile();
            String path = dataFile.location();
            table.io().deleteFile(path);
            LOG.info("event=abort_partial_file_deleted table={}.{} path={}",
                    schemaName, tableName, path);
        } catch (Exception e) {
            LOG.warn("event=abort_partial_file_delete_failed table={}.{} error={}",
                    schemaName, tableName, e.getMessage());
        }
    }

    @Override
    public void close() {
        closeUnpartitionedWriter();
        closePartitionWriters();
    }

    /**
     * Subclass-specific commit metadata collection.
     * Closes writers, collects DataFile descriptors, and serializes
     * them as commit metadata bytes WITHOUT performing catalog commit.
     *
     * @return serialized commit metadata fragments
     */
    protected abstract byte[] collectCommitMetadata();

    /**
     * Returns the unpartitioned writer. Only valid for unpartitioned tables.
     */
    protected DataWriter<Record> unpartitionedWriter() {
        return writer;
    }

    private DataWriter<Record> initUnpartitionedWriter() {
        String filePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filePath);
        try {
            var builder = Parquet.writeData(outputFile)
                    .schema(parquetSafeSchema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned());
            applyEncryptionProperties(builder);
            return builder.build();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize Parquet writer", e);
        }
    }

    private void closeUnpartitionedWriter() {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException e) {
            // Writer may already be closed by collectCommitMetadata — ignore
        }
    }

    /**
     * Applies Parquet encryption properties to the writer builder if configured.
     * Properties are pre-resolved key bytes passed via IcebergWriteContext.
     */
    private void applyEncryptionProperties(Parquet.DataWriteBuilder builder) {
        encryptionProperties.ifPresent(props ->
                props.forEach(builder::set));
    }

    /**
     * Closes and collects DataFiles from all per-partition writers.
     * Each writer produces one DataFile.
     */
    protected List<DataFile> closeAndCollectPartitionFiles() {
        List<DataFile> files = new ArrayList<>(closedPartitionFiles);
        IOException firstFailure = null;
        for (DataWriter<Record> pw : partitionWriters.values()) {
            try {
                pw.close();
                files.add(pw.toDataFile());
            } catch (IOException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
                LOG.warn("event=partition_writer_close_failed table={}.{} error={}",
                        schemaName, tableName, e.getMessage());
            }
        }
        if (firstFailure != null) {
            throw new UncheckedIOException(
                    "Failed to close one or more partition writers", firstFailure);
        }
        return files;
    }

    /**
     * Returns whether any partitioned records have been written.
     */
    protected boolean hasPartitionedData() {
        return partitionWriters != null && !partitionWriters.isEmpty();
    }

    private void closePartitionWriters() {
        if (partitionWriters == null) {
            return;
        }
        IOException firstFailure = collectWriterCloseFailures();
        if (firstFailure != null) {
            throw new UncheckedIOException(
                    "Failed to close one or more partition writers", firstFailure);
        }
    }

    private IOException collectWriterCloseFailures() {
        IOException firstFailure = null;
        for (DataWriter<Record> pw : partitionWriters.values()) {
            try {
                pw.close();
            } catch (IOException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        return firstFailure;
    }

    /**
     * Converts a {@link TypedRow} to an Iceberg {@link GenericRecord}.
     * Uses parquetSafeSchema for GenericRecord creation (Parquet-compatible types)
     * but iterates writeSchema for type-aware coercion of v3 special types.
     */
    protected GenericRecord convertRow(TypedRow row) {
        GenericRecord record = GenericRecord.create(parquetSafeSchema);
        for (Types.NestedField field : writeSchema.columns()) {
            ColumnValue cv = row.containsColumn(field.name())
                    ? row.get(field.name()) : new ColumnValue.NullValue();
            Object value = ColumnValue.toRaw(cv);
            record.setField(field.name(), IcebergValueCoercion.coerceValue(value, field.type()));
        }
        return record;
    }
}
