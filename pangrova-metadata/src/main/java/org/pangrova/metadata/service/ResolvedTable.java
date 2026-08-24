package org.pangrova.metadata.service;

import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

/**
 * Result of table resolution: combines handle, metadata, and statistics from a catalog.
 */
public record ResolvedTable(
        QualifiedTableRef tableRef,
        ConnectorTableHandle tableHandle,
        ConnectorTableMetadata tableMetadata,
        TableStatistics statistics) {
}
