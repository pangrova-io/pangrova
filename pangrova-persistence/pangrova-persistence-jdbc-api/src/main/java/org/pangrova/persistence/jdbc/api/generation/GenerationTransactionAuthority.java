package org.pangrova.persistence.jdbc.api.generation;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.job.GenerationDispatchSpec;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Current domain authority on an artifact caller's transaction. Implementations
 * acquire the authorization gate before tenant, table-policy and model checks.
 * They must use the supplied connection without committing, rolling back or
 * replacing it. Execution-owner and job locks are acquired afterwards by the caller.
 */
@Internal
public interface GenerationTransactionAuthority {
    /** Reads and validates the exact persisted tenant/job dispatch specification. */
    GenerationDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
            throws SQLException;

    /** Validates an explicit compiler-observed specification under current domain authority. */
    void requireCurrent(Connection connection, GenerationDispatchSpec spec) throws SQLException;
}
