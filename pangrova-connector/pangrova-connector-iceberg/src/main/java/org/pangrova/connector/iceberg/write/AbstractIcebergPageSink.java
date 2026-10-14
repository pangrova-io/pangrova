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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

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
abstract sealed class AbstractIcebergPageSink implements ColumnarPageSink
        permits IcebergPageSink, IcebergOverwritePageSink {

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
    private boolean stopped;
    private byte[] commitMetadata;
    private final Map<DataWriter<Record>, String> ownedWriterPaths = new IdentityHashMap<>();
    private final Set<DataWriter<Record>> closedWriters =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<String> ownedFilePaths = new LinkedHashSet<>();
    private final Set<String> deletedFilePaths = new LinkedHashSet<>();

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
                Object coerced = IcebergValueCoercion.coerceValue(value, fields.get(column).type());
                if (fields.get(column).type().typeId() != org.apache.iceberg.types.Type.TypeID.UNKNOWN) {
                    record.setField(fields.get(column).name(), coerced);
                }
            }
            writeRecord(record);
        }
    }

    @Override
    public void appendNativePage(NativePage page) {
        appendPage(NativePageConverter.toPage(page));
    }

    private void writeRecord(GenericRecord record) {
        if (stopped) throw new IllegalStateException("Iceberg write sink has stopped accepting rows");
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
        try {
            closeWriter(lruEntry.getValue());
            closedPartitionFiles.add(lruEntry.getValue().toDataFile());
            iterator.remove();
            // Preserve the exact path for abort, not the closed encoder and its
            // buffers. The live writer set remains bounded by the LRU limit.
            ownedWriterPaths.remove(lruEntry.getValue());
            closedWriters.remove(lruEntry.getValue());
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
                    .forTable(table)
                    .schema(parquetSafeSchema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(spec)
                    .withPartition(key);
            applyEncryptionProperties(builder);
            DataWriter<Record> result = builder.build();
            ownedWriterPaths.put(result, filePath);
            ownedFilePaths.add(filePath);
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize partition writer", e);
        }
    }

    @Override
    public byte[] finishWithMetadata() {
        if (aborted) {
            return new byte[0];
        }
        if (finished) return commitMetadata.clone();
        if (stopped) throw new IllegalStateException("Iceberg write sink cannot finish after failure");
        stopped = true;
        commitMetadata = collectCommitMetadata();
        finished = true;
        return commitMetadata.clone();
    }

    @Override
    public void abort() {
        if (finished || aborted) {
            return;
        }
        stopped = true;
        deletePartialFiles();
        aborted = true;
    }

    /**
     * Deletes partial data files written by this sink that have not been committed.
     * Called during abort to prevent orphan files from accumulating in storage.
     */
    private void deletePartialFiles() {
        // Do not delete a file until all live writers have actually closed.
        close();
        Throwable failure = null;
        for (String path : ownedFilePaths) {
            try {
                deleteWriterFile(path);
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
    }

    private void deleteWriterFile(String path) {
        if (deletedFilePaths.contains(path)) return;
        table.io().deleteFile(path);
        deletedFilePaths.add(path);
        LOG.info("event=abort_partial_file_deleted table={}.{} path={}",
                schemaName, tableName, path);
    }

    @Override
    public void close() {
        stopped = true;
        visitAllWriters(dataWriter -> {
            try {
                closeWriter(dataWriter);
            } catch (IOException failure) {
                throw new UncheckedIOException("Failed to close Iceberg writer", failure);
            }
        });
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

    /** Shares the actual close progress with abort and final resource release. */
    protected void closeUnpartitionedWriterForCommit() throws IOException {
        closeWriter(writer);
    }

    private DataWriter<Record> initUnpartitionedWriter() {
        String filePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filePath);
        try {
            var builder = Parquet.writeData(outputFile)
                    .forTable(table)
                    .schema(parquetSafeSchema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned());
            applyEncryptionProperties(builder);
            DataWriter<Record> result = builder.build();
            ownedWriterPaths.put(result, filePath);
            ownedFilePaths.add(filePath);
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize Parquet writer", e);
        }
    }

    private void closeWriter(DataWriter<Record> dataWriter) throws IOException {
        if (closedWriters.contains(dataWriter)) return;
        dataWriter.close();
        closedWriters.add(dataWriter);
    }

    private void visitAllWriters(Consumer<DataWriter<Record>> operation) {
        Throwable failure = null;
        for (DataWriter<Record> dataWriter : ownedWriterPaths.keySet()) {
            try {
                operation.accept(dataWriter);
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
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
                closeWriter(pw);
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
            Object coerced = IcebergValueCoercion.coerceValue(value, field.type());
            if (field.type().typeId() != org.apache.iceberg.types.Type.TypeID.UNKNOWN) {
                record.setField(field.name(), coerced);
            }
        }
        return record;
    }
}
