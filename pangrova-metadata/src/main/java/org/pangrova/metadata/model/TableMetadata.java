package org.pangrova.metadata.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Unified table metadata: catalog/schema/table identity, columns, partitioning, sort order, and properties.
 */
public record TableMetadata(
        String catalog,
        String schema,
        String table,
        List<ColumnMetadata> columns,
        Map<String, String> partitionSpec,
        List<String> sortOrder,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) {

    public TableMetadata {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
        columns = columns == null ? List.of() : List.copyOf(columns);
        partitionSpec = partitionSpec == null ? Map.of() : Map.copyOf(partitionSpec);
        sortOrder = sortOrder == null ? List.of() : List.copyOf(sortOrder);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }
}
