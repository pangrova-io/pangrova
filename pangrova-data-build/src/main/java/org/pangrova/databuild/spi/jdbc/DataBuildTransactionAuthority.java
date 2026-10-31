package org.pangrova.databuild.spi.jdbc;

import org.pangrova.databuild.api.build.DataBuildDispatchSpec;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Current domain authority on an artifact caller's transaction. Implementations
 * acquire the authorization gate before tenant, table-policy and model checks.
 * They must use the supplied connection without committing, rolling back or
 * replacing it. Execution-owner and job locks are acquired afterwards by the caller.
 */
public interface DataBuildTransactionAuthority {
    /** Reads and validates the exact persisted tenant/job dispatch specification. */
    DataBuildDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
            throws SQLException;

    /** Validates an explicit compiler-observed specification under current domain authority. */
    void requireCurrent(Connection connection, DataBuildDispatchSpec spec) throws SQLException;
}
