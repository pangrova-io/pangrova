package org.pangrova.connector.spi.write;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.write.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;

import java.util.List;

/**
 * SPI contract for writing data records to physical data files.
 *
 * <p>Connector modules provide implementations backed by their native write APIs
 * (e.g., Paimon {@code TableWrite}, Iceberg {@code PageSink}). The streaming module
 * consumes this contract without coupling to specific connector internals.</p>
 *
 * <p>Lifecycle: the caller owns the sink and must call {@link #close()} when done.
 * Implementations must be safe for sequential use from a single thread.</p>
 */
@Experimental
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
