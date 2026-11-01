package org.pangrova.persistence.jdbc.query.coordination;

import org.pangrova.spi.query.coordination.ExecutionAnchor;
import org.pangrova.spi.query.coordination.ExecutionOwnerLostException;
import org.pangrova.spi.query.coordination.ExecutionOwnerLostException.Reason;
import org.pangrova.persistence.api.query.transaction.ExecutionTransactionFence;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** PostgreSQL execution ownership check using only the supplied transaction and its database clock. */
public final class JdbcExecutionTransactionFence implements ExecutionTransactionFence {
    @Override
    public ExecutionAnchor requireCurrent(Connection connection, String executionId, String ownerId,
            String ownerIncarnation, long fencingToken) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        requireText(executionId, "executionId");
        requireText(ownerId, "ownerId");
        requireText(ownerIncarnation, "ownerIncarnation");
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencingToken must be positive");
        }
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("Execution publication fence requires a transaction");
        }
        ExecutionAnchor anchor;
        try (var statement = connection.prepareStatement(
                "SELECT owner_id,owner_incarnation,fencing_token,acquired_at,expires_at "
                        + "FROM execution_anchor WHERE execution_id=? FOR SHARE")) {
            statement.setString(1, executionId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new ExecutionOwnerLostException(Reason.ABSENT);
                }
                anchor = new ExecutionAnchor(executionId, result.getString("owner_id"),
                        result.getString("owner_incarnation"), result.getLong("fencing_token"),
                        result.getTimestamp("acquired_at").toInstant(), result.getTimestamp("expires_at").toInstant());
            }
        }
        if (!anchor.ownerId().equals(ownerId) || !anchor.ownerIncarnation().equals(ownerIncarnation)
                || anchor.fencingToken() != fencingToken) {
            throw new ExecutionOwnerLostException(Reason.REPLACED);
        }
        // LockRows may evaluate SELECT projections before waiting for a lock. Sample
        // wall-clock time in a separate statement after the exact anchor row is held.
        try (var statement = connection.prepareStatement("SELECT clock_timestamp()");
             var time = statement.executeQuery()) {
            if (!time.next()) {
                throw new SQLException("Execution fence database clock returned no row");
            }
            if (!anchor.expiresAt().isAfter(time.getTimestamp(1).toInstant())) {
                throw new ExecutionOwnerLostException(Reason.EXPIRED);
            }
        }
        return anchor;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
