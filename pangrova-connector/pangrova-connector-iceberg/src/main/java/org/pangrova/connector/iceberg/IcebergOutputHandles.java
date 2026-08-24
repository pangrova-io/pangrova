package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Builds {@link IcebergOutputTableHandle}s for the distributed write path
 * (insert, overwrite, delete) and for crash-recovery reconstruction,
 * capturing the table's live metadata and base snapshot for conflict
 * detection at commit time.
 */
final class IcebergOutputHandles {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergOutputHandles.class);

    private IcebergOutputHandles() {}

    /** Builds an output handle for an INSERT distributed write. */
    static ConnectorOutputTableHandle forInsert(IcebergTableHandle handle, Table icebergTable) {
        return build(handle.schemaName(), handle.tableName(),
                icebergTable, IcebergOutputTableHandle.MODE_INSERT);
    }

    /** Builds an output handle for an OVERWRITE distributed write. */
    static ConnectorOutputTableHandle forOverwrite(IcebergTableHandle handle, Table icebergTable) {
        return build(handle.schemaName(), handle.tableName(),
                icebergTable, IcebergOutputTableHandle.MODE_OVERWRITE);
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
        return new IcebergOutputTableHandle(
                handle.schemaName(), handle.tableName(), icebergTable.location(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                formatVersion,
                currentSnapshotId(icebergTable),
                IcebergOutputTableHandle.MODE_DELETE);
    }

    /**
     * A-COMMIT-01: rebuilds an output handle for crash recovery from the live table metadata,
     * mirroring {@code beginInsert/beginOverwrite/beginDelete} for the current table and the
     * recorded {@code writeMode}. Throws {@code NoSuchTableException} if the table is gone (the
     * recovering reconciler then aborts the intent).
     */
    static ConnectorOutputTableHandle reconstruct(Catalog catalog,
                                                  SchemaTableName tableName, String writeMode) {
        TableIdentifier tableId = TableIdentifier.of(tableName.schema(), tableName.table());
        Table icebergTable = catalog.loadTable(tableId);
        return new IcebergOutputTableHandle(
                tableName.schema(), tableName.table(), icebergTable.location(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                IcebergFormatVersions.extract(icebergTable),
                currentSnapshotId(icebergTable),
                normalizeReconstructMode(writeMode));
    }

    private static IcebergOutputTableHandle build(String schemaName, String tableName,
                                                  Table icebergTable, String writeMode) {
        return new IcebergOutputTableHandle(
                schemaName, tableName, icebergTable.location(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                IcebergFormatVersions.extract(icebergTable),
                currentSnapshotId(icebergTable),
                writeMode);
    }

    private static long currentSnapshotId(Table icebergTable) {
        return icebergTable.currentSnapshot() != null
                ? icebergTable.currentSnapshot().snapshotId()
                : IcebergOutputTableHandle.NO_SNAPSHOT;
    }

    private static String normalizeReconstructMode(String writeMode) {
        if (writeMode == null) {
            return IcebergOutputTableHandle.MODE_INSERT;
        }
        return switch (writeMode.toUpperCase(Locale.ROOT)) {
            case IcebergOutputTableHandle.MODE_OVERWRITE -> IcebergOutputTableHandle.MODE_OVERWRITE;
            case IcebergOutputTableHandle.MODE_DELETE -> IcebergOutputTableHandle.MODE_DELETE;
            default -> IcebergOutputTableHandle.MODE_INSERT;
        };
    }
}
