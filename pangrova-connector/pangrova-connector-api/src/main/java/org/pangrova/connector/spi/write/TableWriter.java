package org.pangrova.connector.spi.write;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.write.TableFormat;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.write.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.connector.api.write.WriteResult;

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
     * committed. Implementations that cannot verify commit properties must fail
     * explicitly; an empty result means the property was verified absent.</p>
     *
     * @param tableName fully qualified table name
     * @param key       the property key to read
     * @return the property value, or empty if verified absent
     * @throws UnsupportedOperationException when commit properties cannot be read
     */
    default Optional<String> readCommitProperty(String tableName, String key) {
        throw new UnsupportedOperationException("Commit property reads are not supported by this table writer");
    }
}
