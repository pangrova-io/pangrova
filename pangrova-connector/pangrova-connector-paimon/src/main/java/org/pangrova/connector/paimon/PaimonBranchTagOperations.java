package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Branch and tag management operations for Paimon tables.
 */
final class PaimonBranchTagOperations {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonBranchTagOperations.class);

    private PaimonBranchTagOperations() {}

    static void createBranch(Table table, PaimonTableHandle handle, String branchName,
            org.pangrova.connector.spi.metadata.BranchCreationOptions options) {
        java.util.Objects.requireNonNull(options, "options");
        if (options.sourceSnapshotId().isPresent() || options.maxReferenceAgeMillis().isPresent()) {
            throw new UnsupportedOperationException(
                    "Paimon branch creation does not support a snapshot source or reference lifetime");
        }
        if (options.sourceTag().isPresent()) table.createBranch(branchName, options.sourceTag().orElseThrow());
        else table.createBranch(branchName);
        LOG.info("event=branch_created table={}.{} branch={}",
                handle.schemaName(), handle.tableName(), branchName);
    }

    static void dropBranch(Table table, PaimonTableHandle handle, String branchName) {
        table.deleteBranch(branchName);
        LOG.info("event=branch_dropped table={}.{} branch={}",
                handle.schemaName(), handle.tableName(), branchName);
    }

    static void createTag(Table table, PaimonTableHandle handle, String tagName) {
        table.createTag(tagName);
        LOG.info("event=tag_created table={}.{} tag={}",
                handle.schemaName(), handle.tableName(), tagName);
    }

    static void dropTag(Table table, PaimonTableHandle handle, String tagName) {
        table.deleteTag(tagName);
        LOG.info("event=tag_dropped table={}.{} tag={}",
                handle.schemaName(), handle.tableName(), tagName);
    }
}
