package org.pangrova.generation.spi.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.spi.query.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;

/** Exact durable job routing and current execution ownership on a caller-owned transaction. */
@Internal
public interface GenerationExecutionTransactionFence {
    /**
     * Holds the actual backend owner after current authority and before job or
     * descriptor locks. The expected identity must match the persisted tenant/job
     * route and execution owner. The supplied connection must have auto-commit
     * disabled; implementations never replace, commit, roll back or close it,
     * or change its auto-commit state.
     */
    ExecutionAnchor requireCurrent(Connection connection, String tenantId, String jobId,
            GenerationExecutionOwner expected) throws SQLException;

    /** Verifies that the exact anchor belongs to this job and still has current backend ownership. */
    void requireAnchor(Connection connection, String tenantId, String jobId, ExecutionAnchor anchor)
            throws SQLException;

    /**
     * Validates the persisted publication owner and exact job route using only
     * the supplied transaction. The first check precedes job/descriptor locks;
     * the publisher repeats it after potentially blocking writes and before commit,
     * while retaining the already acquired execution lock. A backend must validate
     * expiry against a clock sampled after acquiring the actual owner lock.
     */
    void requirePublication(Connection connection, String tenantId, String jobId) throws SQLException;
}
