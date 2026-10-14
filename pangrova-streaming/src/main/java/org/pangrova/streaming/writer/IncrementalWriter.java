package org.pangrova.streaming.writer;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Incremental write buffering layer on top of TableWriter SPI.
 * Part of the declarative incremental data transformation subsystem.
 * Flush triggers: maxBufferBytes, maxBufferRows, maxBufferTime.
 */
public final class IncrementalWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalWriter.class);

    private final String tableName;
    private final TableWriter tableWriter;
    private final IncrementalWriterConfig config;
    private final List<TypedRow> buffer;
    private final long cachedRowSizeEstimate;
    private long bufferBytes;
    private Instant lastFlushTime;

    public IncrementalWriter(String tableName, TableWriter tableWriter,
                           IncrementalWriterConfig config) {
        this(tableName, tableWriter, config, List.of());
    }

    /**
     * @param typedColumns column descriptors from ConnectorTableMetadata for
     *                     accurate per-row size estimation. Empty list falls
     *                     back to a conservative default.
     */
    public IncrementalWriter(String tableName, TableWriter tableWriter,
                           IncrementalWriterConfig config,
                           List<ConnectorTableMetadata.Column> typedColumns) {
        this.tableName = tableName;
        this.tableWriter = tableWriter;
        this.config = config;
        this.buffer = new ArrayList<>();
        this.bufferBytes = 0;
        this.lastFlushTime = Instant.now();
        this.cachedRowSizeEstimate = computeRowSizeEstimate(typedColumns);
    }

    /** Buffers a single row, flushing automatically when buffer limits are reached. */
    public void write(TypedRow record) {
        if (bufferBytes >= config.maxBufferBytes()) {
            LOG.warn("event=backpressure_triggered tableName={} bufferBytes={} maxBufferBytes={}",
                    tableName, bufferBytes, config.maxBufferBytes());
            flush();
        }
        buffer.add(record);
        bufferBytes += estimateSize(record);
        if (shouldFlush()) {
            flush();
        }
    }

    /** Buffers a batch of rows, flushing automatically when buffer limits are reached. */
    public void writeBatch(List<TypedRow> records) {
        for (TypedRow record : records) {
            if (bufferBytes >= config.maxBufferBytes()) {
                LOG.warn("event=backpressure_triggered tableName={} bufferBytes={} maxBufferBytes={}",
                        tableName, bufferBytes, config.maxBufferBytes());
                flush();
            }
            buffer.add(record);
            bufferBytes += estimateSize(record);
        }
        if (shouldFlush()) {
            flush();
        }
    }

    /** Writes all buffered rows to the target table and resets the buffer. */
    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        int recordCount = buffer.size();
        List<WriteRecord> writeRecords = buffer.stream()
            .map(data -> WriteRecord.fromClient(data, WriteRecord.WriteOperation.INSERT))
            .toList();
        try {
            tableWriter.write(tableName, writeRecords, WriteMode.APPEND, WriteContext.selfManaged());
        } catch (Exception e) {
            LOG.error("event=flush_failed tableName={} recordCount={} error={}",
                    tableName, recordCount, e.getMessage(), e);
            throw e;
        }
        resetBuffer();
    }

    public boolean shouldFlush() {
        if (bufferBytes >= config.maxBufferBytes()) return true;
        if (buffer.size() >= config.maxBufferRows()) return true;
        return isTimeElapsed();
    }

    @Override
    public void close() {
        flush();
    }

    public int bufferSize() {
        return buffer.size();
    }

    public long bufferBytes() {
        return bufferBytes;
    }

    private void resetBuffer() {
        buffer.clear();
        bufferBytes = 0;
        lastFlushTime = Instant.now();
    }

    private boolean isTimeElapsed() {
        return Instant.now().isAfter(lastFlushTime.plus(config.maxBufferTime()));
    }

    private long estimateSize(TypedRow record) {
        return cachedRowSizeEstimate;
    }

    private static long computeRowSizeEstimate(List<ConnectorTableMetadata.Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return 128; // conservative default when schema unavailable
        }
        long size = 0;
        for (ConnectorTableMetadata.Column col : columns) {
            size += estimateColumnSize(col.type());
        }
        return size;
    }

    private static long estimateColumnSize(PangrovaType type) {
        return switch (type) {
            case BOOLEAN -> 1;
            case TINYINT -> 1;
            case SMALLINT -> 2;
            case INTEGER, FLOAT, DATE -> 4;
            case BIGINT, DOUBLE, TIME, TIMESTAMP, TIMESTAMP_TZ,
                 INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME -> 8;
            case DECIMAL -> 16;
            case UUID, FIXED -> 16;
            case VARCHAR -> 64;
            case VARBINARY -> 128;
            case STRUCT, ARRAY, MAP -> 256;
            case VECTOR -> 512;
            case VARIANT -> 256;
            case GEOMETRY, GEOGRAPHY -> 128;
            case TIMESTAMP_NS -> 8;
            case UNKNOWN -> 0;
        };
    }
}
