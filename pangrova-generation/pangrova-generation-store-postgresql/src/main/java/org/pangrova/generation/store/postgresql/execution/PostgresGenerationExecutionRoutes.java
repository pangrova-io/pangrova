package org.pangrova.generation.store.postgresql.execution;

import org.pangrova.generation.api.job.GenerationJobKey;
import org.pangrova.common.api.annotation.Internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/** Exact durable routing associations owned by Artifact, using the caller's transaction. */
@Internal
public final class PostgresGenerationExecutionRoutes {
    private PostgresGenerationExecutionRoutes() {}

    /** Persists the exact tenant/job route before this job's execution is issued. */
    public static void insertRoute(Connection connection, String tenantId, String jobId) throws SQLException {
        var key = new GenerationJobKey(tenantId, jobId);
        try (var statement = connection.prepareStatement(
                "INSERT INTO artifact_execution_routes(execution_id,tenant_id,job_id) VALUES(?,?,?)")) {
            statement.setString(1, key.executionId());
            statement.setString(2, tenantId);
            statement.setString(3, jobId);
            statement.executeUpdate();
        }
    }

    /** Resolves only a persisted exact tenant/job association, never an identifier hash as authority. */
    public static Optional<GenerationJobKey> findRoute(Connection connection, String executionId)
            throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT tenant_id,job_id FROM artifact_execution_routes WHERE execution_id=?")) {
            statement.setString(1, executionId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                var key = new GenerationJobKey(result.getString(1), result.getString(2));
                if (!key.executionId().equals(executionId) || result.next()) {
                    throw new IllegalStateException("ambiguous or corrupt artifact execution association");
                }
                return Optional.of(key);
            }
        }
    }

}
