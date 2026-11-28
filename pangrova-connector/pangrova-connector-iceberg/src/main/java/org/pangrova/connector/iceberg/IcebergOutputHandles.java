package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.enums.IcebergOverwriteScope;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.HasTableOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Builds {@link IcebergOutputTableHandle}s for the distributed write path
 * (insert, overwrite, delete), capturing table metadata and the original base
 * snapshot. The output codec carries this allocation through durable recovery.
 */
final class IcebergOutputHandles {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergOutputHandles.class);

    private IcebergOutputHandles() {}

    /** Builds an output handle for an INSERT distributed write. */
    static ConnectorOutputTableHandle forInsert(IcebergTableHandle handle, Table icebergTable) {
        return build(handle.schemaName(), handle.tableName(),
                icebergTable, IcebergOutputTableHandle.MODE_INSERT);
    }

    /** Builds an output handle for an OVERWRITE distributed write that replaces the whole table. */
    static ConnectorOutputTableHandle forOverwrite(IcebergTableHandle handle, Table icebergTable) {
        return build(handle.schemaName(), handle.tableName(), icebergTable,
                IcebergOutputTableHandle.MODE_OVERWRITE, IcebergOverwriteScope.WHOLE_TABLE,
                Map.of());
    }

    static ConnectorOutputTableHandle forOverwrite(
            IcebergTableHandle handle, Table table, Map<String, String> staticPartitions) {
        if (staticPartitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Static partition overwrite requires at least one partition");
        }
        return build(handle.schemaName(), handle.tableName(), table,
                IcebergOutputTableHandle.MODE_OVERWRITE, IcebergOverwriteScope.STATIC_PARTITION,
                staticPartitions);
    }

    /** Builds an output handle for a CDC MERGE distributed write. */
    static ConnectorOutputTableHandle forMerge(IcebergTableHandle handle, Table icebergTable) {
        return build(handle.schemaName(), handle.tableName(), icebergTable,
                IcebergOutputTableHandle.MODE_MERGE);
    }

    /** Builds an output handle for a DELETE distributed write. */
    static ConnectorOutputTableHandle forDelete(IcebergTableHandle handle, Table icebergTable) {
        int formatVersion = IcebergFormatVersions.extract(icebergTable);
        if (formatVersion < 2) {
            LOG.info("event=begin_delete table={}.{} format_version={} deletion_mode=COPY_ON_WRITE",
                    handle.schemaName(), handle.tableName(), formatVersion);
        } else if (formatVersion >= 3) {
            LOG.info("event=begin_delete table={}.{} format_version={} deletion_mode=DELETION_VECTOR",
                    handle.schemaName(), handle.tableName(), formatVersion);
        }
        return build(handle.schemaName(), handle.tableName(), icebergTable,
                IcebergOutputTableHandle.MODE_DELETE);
    }

    private static IcebergOutputTableHandle build(String schemaName, String tableName,
                                                  Table icebergTable, String writeMode) {
        return build(schemaName, tableName, icebergTable, writeMode,
                IcebergOverwriteScope.NONE, Map.of());
    }

    private static IcebergOutputTableHandle build(String schemaName, String tableName,
            Table table, String writeMode, IcebergOverwriteScope overwriteScope,
            Map<String, String> staticPartitions) {
        if (!(table instanceof HasTableOperations sdkTable)) {
            throw new IllegalStateException("Iceberg write allocation requires SDK TableOperations");
        }
        // Every caller resolves the current catalog table before building the allocation.
        var metadata = sdkTable.operations().current();
        IcebergStaticOverwriteScope.filter(metadata.schema(), metadata.spec(), staticPartitions);
        long snapshotId = metadata.currentSnapshot() == null
                ? IcebergOutputTableHandle.NO_SNAPSHOT : metadata.currentSnapshot().snapshotId();
        return new IcebergOutputTableHandle(
                schemaName, tableName, metadata.uuid(), metadata.location(),
                SchemaParser.toJson(metadata.schema()), PartitionSpecParser.toJson(metadata.spec()),
                metadata.formatVersion(), snapshotId,
                writeMode, overwriteScope, staticPartitions,
                IcebergDeclarationProperties.capture(metadata.properties()));
    }

    static String tableUuid(Table icebergTable) {
        java.util.UUID uuid = icebergTable.uuid();
        if (uuid == null) {
            throw new IllegalStateException("Iceberg table UUID is unavailable for a distributed write");
        }
        return uuid.toString();
    }
}
