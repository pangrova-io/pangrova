package org.pangrova.spi.connector.data.write.model;

import org.pangrova.spi.annotation.Experimental;

import java.time.Instant;
import java.util.Objects;

import org.pangrova.spi.connector.data.TypedRow;

/**
 * Unified write record for table write operations.
 *
 * <p>CDC-specific conversion (fromCdc) lives in the streaming module
 * to avoid pulling CDC dependencies into the SPI layer.</p>
 */
@Experimental
public record WriteRecord(
        TypedRow data,
        WriteOperation operation,
        Instant eventTime
) {
    public enum WriteOperation { INSERT, UPDATE, DELETE }

    public static WriteRecord fromClient(TypedRow data, WriteOperation op) {
        Objects.requireNonNull(data, "data must not be null");
        Objects.requireNonNull(op, "operation must not be null");
        return new WriteRecord(data, op, Instant.now());
    }
}
