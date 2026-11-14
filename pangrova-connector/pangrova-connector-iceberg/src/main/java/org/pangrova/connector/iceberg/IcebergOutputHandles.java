package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.enums.IcebergOverwriteScope;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Builds {@link IcebergOutputTableHandle}s for the distributed write path
 * (insert, overwrite, delete) and for crash-recovery reconstruction,
 * capturing the table metadata and the original base snapshot for conflict detection at commit
 * time. Recovery must pass the persisted base snapshot rather than observing a new one.
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
                Map.of(), null);
    }

    static ConnectorOutputTableHandle forOverwrite(
            IcebergTableHandle handle, Table table, Map<String, String> staticPartitions) {
        if (staticPartitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Static partition overwrite requires at least one partition");
        }
        IcebergStaticOverwriteScope.filter(table.schema(), table.spec(), staticPartitions);
        return build(handle.schemaName(), handle.tableName(), table,
                IcebergOutputTableHandle.MODE_OVERWRITE, IcebergOverwriteScope.STATIC_PARTITION,
                staticPartitions, null);
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

    /**
     * A-COMMIT-01: rebuilds an output handle for crash recovery from the live table metadata,
     * mirroring {@code beginInsert/beginOverwrite/beginDelete} for the current table and the
     * recorded {@code writeMode}. This legacy overload observes the current snapshot and is not
     * suitable for crash recovery. Throws {@code NoSuchTableException} if the table is gone.
     */
    static ConnectorOutputTableHandle reconstruct(Catalog catalog,
                                                  SchemaTableName tableName, String writeMode) {
        TableIdentifier tableId = TableIdentifier.of(tableName.schema(), tableName.table());
        Table icebergTable = catalog.loadTable(tableId);
        return reconstruct(icebergTable, tableName, writeMode, currentSnapshotId(icebergTable));
    }

    static ConnectorOutputTableHandle reconstruct(
            Catalog catalog,
            SchemaTableName tableName,
            String writeMode,
            long baseSnapshotId) {
        if (IcebergOutputTableHandle.MODE_OVERWRITE.equals(writeMode)) {
            // An overwrite's replacement scope cannot be derived from live metadata. Rebuilding it
            // here would silently widen a static-partition replacement, so recovery must decode
            // the journaled output handle instead.
            throw new UnsupportedOperationException(
                    "Iceberg OVERWRITE recovery requires the journaled output handle");
        }
        TableIdentifier tableId = TableIdentifier.of(tableName.schema(), tableName.table());
        Table icebergTable = catalog.loadTable(tableId);
        return reconstruct(icebergTable, tableName, writeMode, baseSnapshotId);
    }

    private static ConnectorOutputTableHandle reconstruct(
            Table icebergTable,
            SchemaTableName tableName,
            String writeMode,
            long baseSnapshotId) {
        return build(tableName.schema(), tableName.table(), icebergTable,
                normalizeReconstructMode(writeMode), IcebergOverwriteScope.NONE,
                Map.of(), baseSnapshotId);
    }

    private static IcebergOutputTableHandle build(String schemaName, String tableName,
                                                  Table icebergTable, String writeMode) {
        return build(schemaName, tableName, icebergTable, writeMode,
                IcebergOverwriteScope.NONE, Map.of(), null);
    }

    private static IcebergOutputTableHandle build(String schemaName, String tableName,
            Table table, String writeMode, IcebergOverwriteScope overwriteScope,
            Map<String, String> staticPartitions, Long originalSnapshotId) {
        if (!(table instanceof HasTableOperations sdkTable)) {
            throw new IllegalStateException("Iceberg write allocation requires SDK TableOperations");
        }
        var metadata = sdkTable.operations().refresh();
        IcebergStaticOverwriteScope.filter(metadata.schema(), metadata.spec(), staticPartitions);
        long snapshotId = metadata.currentSnapshot() == null
                ? IcebergOutputTableHandle.NO_SNAPSHOT : metadata.currentSnapshot().snapshotId();
        return new IcebergOutputTableHandle(
                schemaName, tableName, metadata.uuid(), metadata.location(),
                SchemaParser.toJson(metadata.schema()), PartitionSpecParser.toJson(metadata.spec()),
                metadata.formatVersion(), originalSnapshotId == null ? snapshotId : originalSnapshotId,
                writeMode, overwriteScope, staticPartitions,
                IcebergDeclarationProperties.capture(metadata.properties()));
    }

    private static long currentSnapshotId(Table icebergTable) {
        return icebergTable.currentSnapshot() != null
                ? icebergTable.currentSnapshot().snapshotId()
                : IcebergOutputTableHandle.NO_SNAPSHOT;
    }

    static String tableUuid(Table icebergTable) {
        java.util.UUID uuid = icebergTable.uuid();
        if (uuid == null) {
            throw new IllegalStateException("Iceberg table UUID is unavailable for a distributed write");
        }
        return uuid.toString();
    }

    private static String normalizeReconstructMode(String writeMode) {
        if (writeMode == null) {
            throw new IllegalArgumentException("Distributed write recovery mode is missing");
        }
        return switch (writeMode) {
            case IcebergOutputTableHandle.MODE_INSERT -> IcebergOutputTableHandle.MODE_INSERT;
            case IcebergOutputTableHandle.MODE_OVERWRITE -> IcebergOutputTableHandle.MODE_OVERWRITE;
            case IcebergOutputTableHandle.MODE_DELETE -> IcebergOutputTableHandle.MODE_DELETE;
            default -> throw new IllegalArgumentException(
                    "Unsupported distributed write recovery mode: " + writeMode);
        };
    }
}
