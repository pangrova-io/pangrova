package org.pangrova.metadata.api.resolution;

import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import java.util.Optional;
import java.util.UUID;

/**
 * Result of table resolution: combines handle, metadata, statistics, and control
 * ownership.
 *
 * <p>OBSERVED tables have no stable id. A controlled record's id and version are
 * the values that admission and maintenance must fence against.</p>
 */
public record ResolvedTable(
        QualifiedTableRef tableRef,
        ConnectorTableHandle tableHandle,
        ConnectorTableMetadata tableMetadata,
        TableStatistics statistics,
        OwnershipMode ownershipMode,
        Optional<UUID> tableId,
        Optional<String> versionToken,
        long controlRevision) {
}
