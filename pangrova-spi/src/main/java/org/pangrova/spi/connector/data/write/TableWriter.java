package org.pangrova.spi.connector.data.write;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.connector.data.write.model.WriteResult;

import java.util.List;
import java.util.Optional;

/**
 * SPI contract for unified table write operations (CDC and SQL writes).
 *
 * <p>Implementations are provided by connector modules (e.g., Iceberg, Paimon).
 * The streaming module and coordinator consume this contract without coupling
 * to specific connector internals.</p>
 *
 * @see WriteRecord
 * @see WriteResult
 * @see WriteMode
 */
@Experimental
public interface TableWriter extends AutoCloseable {

    /**
     * Write records to the specified table.
     *
     * @param tableName  fully qualified table name
     * @param records    records to write
     * @param writeMode  append, upsert, or overwrite
     * @param context    transaction and session context
     * @return write result with commit metadata
     */
    WriteResult write(String tableName, List<WriteRecord> records,
                      WriteMode writeMode, WriteContext context);

    /** Table format this writer targets. */
    TableFormat format();

    /**
     * Reads a commit property from the most recent commit of the given table.
     *
     * <p>Used for idempotent recovery: the coordinator embeds a checkpoint in commit
     * properties, and on recovery reads it back to determine if the write was already
     * committed. Implementations that do not support commit properties return empty.</p>
     *
     * @param tableName fully qualified table name
     * @param key       the property key to read
     * @return the property value, or empty if not present or not supported
     */
    default Optional<String> readCommitProperty(String tableName, String key) {
        return Optional.empty();
    }
}
