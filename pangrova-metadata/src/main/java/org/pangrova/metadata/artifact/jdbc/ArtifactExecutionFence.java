package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.metadata.artifact.build.ArtifactBuildJobKey;
import org.pangrova.metadata.artifact.build.ArtifactExecutionOwner;
import org.pangrova.spi.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

/** Same-transaction PostgreSQL execution fence. Lock after authority and before job/descriptor locks. */
@org.pangrova.spi.annotation.Internal
public final class ArtifactExecutionFence {
    private ArtifactExecutionFence() {}

    /** Persists the exact tenant/job route before this job's execution is issued. */
    public static void insertRoute(Connection connection, String tenantId, String jobId) throws SQLException {
        var key = new ArtifactBuildJobKey(tenantId, jobId);
        try (var statement = connection.prepareStatement(
                "INSERT INTO artifact_execution_routes(execution_id,tenant_id,job_id) VALUES(?,?,?)")) {
            statement.setString(1, key.executionId());
            statement.setString(2, tenantId);
            statement.setString(3, jobId);
            statement.executeUpdate();
        }
    }

    /** Resolves only a persisted exact tenant/job association, never an identifier hash as authority. */
    public static Optional<ArtifactBuildJobKey> findRoute(Connection connection, String executionId)
            throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT tenant_id,job_id FROM artifact_execution_routes WHERE execution_id=?")) {
            statement.setString(1, executionId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                var key = new ArtifactBuildJobKey(result.getString(1), result.getString(2));
                if (!key.executionId().equals(executionId) || result.next()) {
                    throw new IllegalStateException("ambiguous or corrupt artifact execution association");
                }
                return Optional.of(key);
            }
        }
    }

    /** Holds the current anchor against takeover until the caller commits or rolls back. */
    public static ExecutionAnchor requireCurrent(Connection connection, String tenantId, String jobId,
            ArtifactExecutionOwner expected) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("artifact execution fence requires a transaction");
        }
        var key = new ArtifactBuildJobKey(tenantId, jobId);
        if (!findRoute(connection, key.executionId()).filter(key::equals).isPresent()) {
            throw new IllegalStateException("artifact execution has no exact durable routing association");
        }
        try (var statement = connection.prepareStatement(
                "SELECT owner_id,owner_incarnation,fencing_token,acquired_at,expires_at FROM execution_anchor "
                        + "WHERE execution_id=? FOR SHARE")) {
            statement.setString(1, key.executionId());
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new LostOwnerException("artifact execution anchor is absent or expired");
                }
                var anchor = new ExecutionAnchor(key.executionId(), result.getString("owner_id"),
                        result.getString("owner_incarnation"), result.getLong("fencing_token"),
                        result.getTimestamp("acquired_at").toInstant(), result.getTimestamp("expires_at").toInstant());
                if (!ArtifactExecutionOwner.from(anchor).equals(expected)) {
                    throw new LostOwnerException("artifact execution owner fence was lost");
                }
                // LockRows can evaluate target expressions before waiting for the row lock.
                // Sample wall-clock time in a separate statement after the exact row is held.
                try (var clock = connection.prepareStatement("SELECT clock_timestamp()");
                     var time = clock.executeQuery()) {
                    if (!time.next() || !anchor.expiresAt().isAfter(time.getTimestamp(1).toInstant())) {
                        throw new LostOwnerException("artifact execution anchor is absent or expired");
                    }
                }
                return anchor;
            }
        }
    }

    /** An explicit stale-owner rejection, distinct from unavailable/corrupt authority or invalid input. */
    public static final class LostOwnerException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private LostOwnerException(String message) {
            super(message);
        }
    }

    /** Checks both the anchor's exact job association and its current execution ownership. */
    public static void requireAnchor(Connection connection, String tenantId, String jobId, ExecutionAnchor anchor)
            throws SQLException {
        if (!new ArtifactBuildJobKey(tenantId, jobId).executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("execution anchor belongs to another artifact job");
        }
        requireCurrent(connection, tenantId, jobId, ArtifactExecutionOwner.from(anchor));
    }

    /** Verifies the exact persisted publication owner's live backend token before any job lock. */
    public static void requirePublication(Connection connection, String tenantId, String jobId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT publication_execution_owner_id,publication_execution_owner_incarnation,"
                        + "publication_execution_owner_token FROM artifact_build_jobs "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("artifact publication has no job");
                }
                requireCurrent(connection, tenantId, jobId, new ArtifactExecutionOwner(
                        result.getString(1), result.getString(2), result.getLong(3)));
            }
        }
    }
}
