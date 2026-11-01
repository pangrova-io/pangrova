package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.spi.connector.metadata.table.ConnectorDerivedQuery;
import org.pangrova.spi.connector.metadata.table.ConnectorDerivedTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

/** Frozen metadata inputs and an ordinary relational aggregation for Iceberg partitions. */
public record IcebergPartitionTableHandle(IcebergTableHandle baseTable,
        List<ConnectorTableMetadata.Column> typedColumns,
        ConnectorDerivedQuery derivedQuery) implements ConnectorDerivedTableHandle {
    public IcebergPartitionTableHandle {
        Objects.requireNonNull(baseTable, "baseTable");
        typedColumns = List.copyOf(Objects.requireNonNull(typedColumns, "typedColumns"));
        Objects.requireNonNull(derivedQuery, "derivedQuery");
        if (typedColumns.isEmpty()) throw new IllegalArgumentException("Partition schema is required");
    }

    public String tableName() { return baseTable.tableName() + "$partitions"; }

    public String sourceIdentity() {
        return DigestUtils.sha256Hex(derivedQuery.sql() + "\0" + derivedQuery.inputs().stream()
                .map(input -> input.binding().sourceIdentity()).collect(Collectors.joining("\0")));
    }
}
