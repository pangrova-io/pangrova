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

    static void createBranch(Table table, PaimonTableHandle handle, String branchName) {
        table.createBranch(branchName);
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
