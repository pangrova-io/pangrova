package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;

/**
 * Provides access to connector-specific virtual tables (system tables).
 *
 * <p>System tables expose internal metadata such as snapshots, files,
 * manifests, partitions, tags, and branches. They are accessed via
 * the {@code table$systemTable} SQL syntax and return results as
 * {@link TypedRow} lists.</p>
 *
 * <p>Each system table declares its column schema via {@link #columns()}
 * and produces rows via {@link #rows()}. The coordinator routes
 * {@code SELECT FROM catalog.schema.table$name} to the appropriate
 * {@code ConnectorSystemTable} returned by
 * {@link ConnectorMetadata#getSystemTable}.</p>
 */
@Experimental
public interface ConnectorSystemTable {

    /**
     * Returns the column names for this system table.
     *
     * @return an unmodifiable list of column names, never null
     */
    List<String> columns();

    /**
     * Returns all rows for this system table.
     *
     * <p>Implementations should return a snapshot of the current state.
     * The coordinator may apply additional WHERE/ORDER BY/LIMIT
     * filtering on the returned rows.</p>
     *
     * @return an unmodifiable list of typed rows, never null
     */
    List<TypedRow> rows();
}
