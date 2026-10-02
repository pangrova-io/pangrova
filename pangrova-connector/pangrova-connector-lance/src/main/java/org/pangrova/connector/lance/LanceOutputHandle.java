package org.pangrova.connector.lance;

import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import java.util.Optional;
import java.util.OptionalLong;

/** Connector allocation pinned to the table incarnation, tenant, schema and visible version. */
public record LanceOutputHandle(LanceTableHandle table, NativeWriteAllocation allocation, String mode)
        implements ConnectorOutputTableHandle {
    public LanceOutputHandle {
        if (!table.datasetUri().equals(allocation.datasetUri()) || table.version() != allocation.readVersion()
                || !table.columns().stream().map(LanceColumn::name).toList().equals(allocation.writeColumnNames())
                || !java.util.Arrays.equals(table.schemaFence(), allocation.schemaFenceSha256())) {
            throw new IllegalArgumentException("Lance allocation differs from table snapshot");
        }
        validateMode(allocation, mode);
    }
    @Override public Optional<SchemaTableName> targetTable() { return Optional.of(table.table()); }
    @Override public String writeMode() { return mode; }
    @Override public OptionalLong writeBaseSnapshotId() { return OptionalLong.of(table.version()); }
    @Override public Optional<NativeWriteAllocation> nativeWriteAllocation() { return Optional.of(allocation); }

    private static void validateMode(NativeWriteAllocation allocation, String mode) {
        if (!("INSERT".equals(mode) || "OVERWRITE".equals(mode))
                || (allocation.operation() == NativeWriteAllocation.Operation.APPEND && !"INSERT".equals(mode))
                || (allocation.operation() == NativeWriteAllocation.Operation.OVERWRITE && !"OVERWRITE".equals(mode))) {
            throw new IllegalArgumentException("Lance physical operation differs from SQL mode");
        }
    }
}
