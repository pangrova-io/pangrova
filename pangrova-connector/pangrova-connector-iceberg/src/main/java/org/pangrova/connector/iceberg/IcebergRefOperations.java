package org.pangrova.connector.iceberg;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.spi.base.ConnectorException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.pangrova.connector.spi.metadata.ConditionalBranchDropResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Executes snapshot-ref management operations (branches and tags) against a
 * live Iceberg table via the {@code manageSnapshots()} API.
 */
final class IcebergRefOperations {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRefOperations.class);

    private IcebergRefOperations() {}

    /** Publishes the selected source and retention in one SDK metadata update. */
    static void createBranch(Table icebergTable, IcebergTableHandle handle, String branchName,
            org.pangrova.connector.spi.metadata.BranchCreationOptions options) {
        java.util.Objects.requireNonNull(options, "options");
        long snapshot;
        if (options.sourceSnapshotId().isPresent()) {
            snapshot = options.sourceSnapshotId().getAsLong();
        } else if (options.sourceTag().isPresent()) {
            var tag = icebergTable.refs().get(options.sourceTag().orElseThrow());
            if (tag == null || !tag.isTag()) {
                throw new IllegalArgumentException("Branch source tag not found: " + options.sourceTag().orElseThrow());
            }
            snapshot = tag.snapshotId();
        } else {
            snapshot = requireCurrentSnapshot(icebergTable, handle, "branch", branchName).snapshotId();
        }
        var update = icebergTable.manageSnapshots().createBranch(branchName, snapshot);
        options.maxReferenceAgeMillis().ifPresent(age -> update.setMaxRefAgeMs(branchName, age));
        update.commit();
        LOG.info("event=branch_created table={}.{} branch={} snapshotId={}",
                handle.schemaName(), handle.tableName(), branchName, snapshot);
    }

    /** Drops an existing branch from the table. */
    static void dropBranch(Table icebergTable, IcebergTableHandle handle, String branchName) {
        icebergTable.manageSnapshots()
                .removeBranch(branchName)
                .commit();
        LOG.info("event=branch_dropped table={}.{} branch={}",
                handle.schemaName(), handle.tableName(), branchName);
    }

    /**
     * Commits the removal against the exact metadata base whose reference was
     * checked. A concurrent ref update invalidates the base and cannot turn a
     * stale TTL decision into a deletion of a newly advanced branch.
     */
    static ConditionalBranchDropResult dropBranchIfSnapshotMatches(
            Catalog catalog, Table icebergTable, IcebergTableHandle handle,
            String branchName, long expectedSnapshotId) {
        if (branchName == null || branchName.isBlank()
                || SnapshotRef.MAIN_BRANCH.equals(branchName) || expectedSnapshotId <= 0) {
            return ConditionalBranchDropResult.REF_CHANGED;
        }
        if (catalog.getClass() == JdbcCatalog.class) {
            // Load through the exact SDK catalog, not caller-provided operations.
            // JDBC commits atomically compare the complete metadata location in
            // SQL, independently of where the table's data files are stored.
            Table jdbcTable = ((JdbcCatalog) catalog).loadTable(
                    TableIdentifier.of(handle.schemaName(), handle.tableName()));
            return dropBranchIfSnapshotMatches(((HasTableOperations) jdbcTable).operations(),
                    handle, branchName, expectedSnapshotId);
        }
        if (!(icebergTable instanceof HasTableOperations tableWithOperations)) {
            throw new UnsupportedOperationException(
                    "Conditional branch removal requires Iceberg TableOperations");
        }
        TableOperations operations = tableWithOperations.operations();
        // RESTTableOperations derives server requirements from metadata updates.
        // A RemoveSnapshotRef update has no AssertRefSnapshotID requirement in
        // Iceberg 1.11.0, so its commit(base, updated) is NOT a branch CAS.
        // Outside the trusted JDBC catalog, admit only the Hadoop implementation
        // whose atomic metadata-version rename has been verified.
        if (operations.getClass() != HadoopTableOperations.class) {
            throw new UnsupportedOperationException(
                    "Conditional branch removal is not verified for "
                            + operations.getClass().getName());
        }
        if (!isVerifiedAtomicRenameLocation(icebergTable.location())) {
            throw new UnsupportedOperationException(
                    "Conditional branch removal requires verified atomic metadata rename: "
                            + icebergTable.location());
        }
        return dropBranchIfSnapshotMatches(operations, handle, branchName, expectedSnapshotId);
    }

    private static boolean isVerifiedAtomicRenameLocation(String location) {
        if (location == null || location.isBlank()) {
            return false;
        }
        try {
            URI uri = URI.create(location);
            if (uri.getScheme() == null) {
                return location.startsWith("/");
            }
            // Local file is admitted for development/testing. HDFS and object
            // store deployments need their own atomicity tests before enabling.
            return "file".equalsIgnoreCase(uri.getScheme())
                    && uri.getAuthority() == null
                    && uri.getPath() != null
                    && uri.getPath().startsWith("/");
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    static ConditionalBranchDropResult dropBranchIfSnapshotMatches(
            TableOperations operations, IcebergTableHandle handle,
            String branchName, long expectedSnapshotId) {
        return commitBranchRemoval(operations, operations.refresh(), handle, branchName, expectedSnapshotId);
    }

    static ConditionalBranchDropResult commitBranchRemoval(
            TableOperations operations, TableMetadata base, IcebergTableHandle handle,
            String branchName, long expectedSnapshotId) {
        if (branchName == null || branchName.isBlank()
                || SnapshotRef.MAIN_BRANCH.equals(branchName) || expectedSnapshotId <= 0) {
            return ConditionalBranchDropResult.REF_CHANGED;
        }
        SnapshotRef ref = base.ref(branchName);
        if (ref == null || !ref.isBranch() || ref.snapshotId() != expectedSnapshotId) {
            return ConditionalBranchDropResult.REF_CHANGED;
        }
        TableMetadata updated = TableMetadata.buildFrom(base).removeRef(branchName).build();
        try {
            operations.commit(base, updated);
        } catch (CommitStateUnknownException e) {
            LOG.warn("event=branch_drop_commit_unknown table={}.{} branch={} snapshotId={}",
                    handle.schemaName(), handle.tableName(), branchName, expectedSnapshotId, e);
            return ConditionalBranchDropResult.COMMIT_STATE_UNKNOWN;
        } catch (CommitFailedException e) {
            LOG.info("event=branch_drop_conflict table={}.{} branch={} snapshotId={}",
                    handle.schemaName(), handle.tableName(), branchName, expectedSnapshotId, e);
            return ConditionalBranchDropResult.RETRYABLE_CONFLICT;
        }
        LOG.info("event=branch_dropped_conditionally table={}.{} branch={} snapshotId={}",
                handle.schemaName(), handle.tableName(), branchName, expectedSnapshotId);
        return ConditionalBranchDropResult.DROPPED;
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
