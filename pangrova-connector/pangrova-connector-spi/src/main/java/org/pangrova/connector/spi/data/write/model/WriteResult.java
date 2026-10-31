package org.pangrova.connector.spi.data.write.model;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Result of a table write operation.
 */
@Experimental
public record WriteResult(
        String tableName,
        long recordCount,
        int dataFilesWritten,
        int positionDeleteFiles,
        int equalityDeleteFiles,
        Instant commitTime
) {}
