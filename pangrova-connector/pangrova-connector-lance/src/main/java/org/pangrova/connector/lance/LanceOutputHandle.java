package org.pangrova.connector.lance;

import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.NativeWriteAllocation;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import java.util.Optional;
import java.util.OptionalLong;

/** Connector allocation pinned to the table incarnation, tenant, schema and visible version. */
public record LanceOutputHandle(LanceTableHandle table, NativeWriteAllocation allocation, String mode,
        org.pangrova.connector.api.metadata.AcceptedSourceSnapshot acceptedBase)
        implements ConnectorOutputTableHandle {
    public LanceOutputHandle {
        if (!table.datasetUri().equals(allocation.datasetUri()) || table.version() != allocation.readVersion()
                || !table.columns().stream().map(LanceColumn::name).toList().equals(allocation.writeColumnNames())
                || !java.util.Arrays.equals(table.schemaFence(), allocation.schemaFenceSha256())) {
            throw new IllegalArgumentException("Lance allocation differs from table snapshot");
        }
        if ((table.version() == 0) != (acceptedBase == null)) {
            throw new IllegalArgumentException("Positive Dataset write requires exact accepted base membership");
        }
        if (acceptedBase != null && (!acceptedBase.tableId().equals(table.tableId())
                || !acceptedBase.tenant().equals(table.tenantId())
                || !acceptedBase.datasetUri().equals(table.datasetUri())
                || acceptedBase.version() != table.version() || acceptedBase.generation() != 1
                || !java.util.Arrays.equals(acceptedBase.schemaSha256(), table.schemaFence()))) {
            throw new IllegalArgumentException("Dataset allocation accepted base differs");
        }
        validateMode(allocation, mode);
    }
    @Override public Optional<SchemaTableName> targetTable() { return Optional.of(table.table()); }
    @Override public String writeMode() { return mode; }
    @Override public OptionalLong writeBaseSnapshotId() { return OptionalLong.of(table.version()); }
    @Override public Optional<NativeWriteAllocation> nativeWriteAllocation() { return Optional.of(allocation); }

    @Override public Optional<org.pangrova.connector.api.metadata.AcceptedSourceSnapshot> acceptedWriteBaseSnapshot() {
        return Optional.ofNullable(acceptedBase);
    }

    private static void validateMode(NativeWriteAllocation allocation, String mode) {
        if (!("INSERT".equals(mode) || "OVERWRITE".equals(mode))
                || (allocation.operation() == NativeWriteAllocation.Operation.APPEND && !"INSERT".equals(mode))
                || (allocation.operation() == NativeWriteAllocation.Operation.OVERWRITE && !"OVERWRITE".equals(mode))) {
            throw new IllegalArgumentException("Lance physical operation differs from SQL mode");
        }
    }
}
