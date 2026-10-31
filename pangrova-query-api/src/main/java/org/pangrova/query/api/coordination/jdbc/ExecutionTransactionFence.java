package org.pangrova.query.api.coordination.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.query.api.coordination.ExecutionOwnerLostException;

import java.sql.Connection;
import java.sql.SQLException;

/** Query-owned execution fencing inside a caller-owned database transaction. */
@Internal
@FunctionalInterface
public interface ExecutionTransactionFence {
    /**
     * Locks and returns the actual current anchor for the exact issued owner tuple.
     * The connection must have auto-commit disabled. The provider neither commits,
     * rolls back nor closes it; its lock remains held until the caller ends the transaction.
     * Expiry is checked against database time sampled after obtaining the anchor lock.
     * Caller-supplied timestamps are deliberately not part of the authority request.
     *
     * @throws ExecutionOwnerLostException if the anchor is absent, replaced or expired
     * @throws SQLException if the database cannot establish the fence
     */
    ExecutionAnchor requireCurrent(Connection connection, String executionId, String ownerId,
            String ownerIncarnation, long fencingToken) throws SQLException;
}
