package org.pangrova.connector.iceberg;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes snapshot-ref management operations (branches and tags) against a
 * live Iceberg table via the {@code manageSnapshots()} API.
 */
final class IcebergRefOperations {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRefOperations.class);

    private IcebergRefOperations() {}

    /** Creates a branch at the table's current snapshot. */
    static void createBranch(Table icebergTable, IcebergTableHandle handle, String branchName) {
        Snapshot current = requireCurrentSnapshot(icebergTable, handle, "branch", branchName);
        icebergTable.manageSnapshots()
                .createBranch(branchName, current.snapshotId())
                .commit();
        LOG.info("event=branch_created table={}.{} branch={} snapshotId={}",
                handle.schemaName(), handle.tableName(), branchName, current.snapshotId());
    }

    /** Drops an existing branch from the table. */
    static void dropBranch(Table icebergTable, IcebergTableHandle handle, String branchName) {
        icebergTable.manageSnapshots()
                .removeBranch(branchName)
                .commit();
        LOG.info("event=branch_dropped table={}.{} branch={}",
                handle.schemaName(), handle.tableName(), branchName);
    }

    /** Creates a tag at the table's current snapshot. */
    static void createTag(Table icebergTable, IcebergTableHandle handle, String tagName) {
        Snapshot current = requireCurrentSnapshot(icebergTable, handle, "tag", tagName);
        icebergTable.manageSnapshots()
                .createTag(tagName, current.snapshotId())
                .commit();
        LOG.info("event=tag_created table={}.{} tag={} snapshotId={}",
                handle.schemaName(), handle.tableName(), tagName, current.snapshotId());
    }

    /** Drops an existing tag from the table. */
    static void dropTag(Table icebergTable, IcebergTableHandle handle, String tagName) {
        icebergTable.manageSnapshots()
                .removeTag(tagName)
                .commit();
        LOG.info("event=tag_dropped table={}.{} tag={}",
                handle.schemaName(), handle.tableName(), tagName);
    }

    private static Snapshot requireCurrentSnapshot(Table icebergTable, IcebergTableHandle handle,
                                                   String refKind, String refName) {
        Snapshot current = icebergTable.currentSnapshot();
        if (current == null) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Cannot create %s '%s' on table %s.%s: table has no snapshots"
                            .formatted(refKind, refName, handle.schemaName(), handle.tableName()));
        }
        return current;
    }
}
