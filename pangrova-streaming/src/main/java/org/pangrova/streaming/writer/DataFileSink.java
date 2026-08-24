package org.pangrova.streaming.writer;

import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;

import java.util.List;

/**
 * Module-internal contract for writing data records to physical data files.
 *
 * <p>This is NOT a public SPI. Connector modules provide implementations backed
 * by their native write APIs (e.g., Paimon {@code TableWrite}, Iceberg
 * {@code PageSink}). The streaming module consumes this contract without
 * coupling to specific connector internals.</p>
 *
 * <p>For the public write SPIs, see:
 * <ul>
 *   <li>{@link org.pangrova.spi.connector.data.ConnectorPageSink} -- the connector-level
 *       2PC write SPI for distributed SQL DML (the canonical write SPI)</li>
 *   <li>{@link org.pangrova.spi.connector.data.write.TableWriter} -- the streaming write SPI
 *       for single-shot CDC/incremental writes (no 2PC needed)</li>
 * </ul>
 *
 * <p>Lifecycle: the caller owns the sink and must call {@link #close()} when done.
 * Implementations must be safe for sequential use from a single thread.</p>
 */
public interface DataFileSink extends AutoCloseable {

    /**
     * Writes a batch of records to the underlying table storage.
     *
     * @param tableName fully qualified table name
     * @param records   records to write, must not be null or empty
     * @param writeMode append, upsert, or overwrite semantics
     * @return number of data files written
     */
    int writeRecords(String tableName, List<WriteRecord> records, WriteMode writeMode);

    @Override
    void close();
}
