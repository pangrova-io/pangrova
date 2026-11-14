package org.pangrova.persistence.generation.postgresql.execution;

import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.exception.GenerationExecutionOwnerLostException;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.exception.ExecutionOwnerLostException;
import org.pangrova.persistence.jdbc.api.query.ExecutionTransactionFence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/** Artifact routing and publication ownership, composed with Query's same-transaction anchor fence. */
@Internal
public final class PostgresGenerationExecutionFence implements GenerationExecutionTransactionFence {
    private final ExecutionTransactionFence executionFence;

    public PostgresGenerationExecutionFence(ExecutionTransactionFence executionFence) {
        this.executionFence = Objects.requireNonNull(executionFence, "executionFence");
    }

    /** Holds the actual owner after current authority and before job/descriptor locks. */
    @Override
    public ExecutionAnchor requireCurrent(Connection connection, String tenantId, String jobId,
            GenerationExecutionOwner expected) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("artifact execution fence requires a transaction");
        }
        Objects.requireNonNull(expected, "expected");
        var key = new GenerationJobKey(tenantId, jobId);
        if (!PostgresGenerationExecutionRoutes.findRoute(connection, key.executionId())
                .filter(key::equals).isPresent()) {
            throw new IllegalStateException("artifact execution has no exact durable routing association");
        }
        try {
            var anchor = executionFence.requireCurrent(connection, key.executionId(), expected.ownerId(),
                    expected.ownerIncarnation(), expected.fencingToken());
            if (!key.executionId().equals(anchor.executionId())
                    || !expected.equals(GenerationExecutionOwner.from(anchor))) {
                throw new IllegalStateException("Query execution fence returned another execution owner");
            }
            return anchor;
        } catch (ExecutionOwnerLostException lost) {
            String message = lost.reason() == ExecutionOwnerLostException.Reason.REPLACED
                    ? "artifact execution owner fence was lost" : "artifact execution anchor is absent or expired";
            throw new GenerationExecutionOwnerLostException(message, lost);
        }
    }

    /** Checks both the anchor's exact job association and its current execution ownership. */
    @Override
    public void requireAnchor(Connection connection, String tenantId, String jobId, ExecutionAnchor anchor)
            throws SQLException {
        if (!new GenerationJobKey(tenantId, jobId).executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("execution anchor belongs to another artifact job");
        }
        requireCurrent(connection, tenantId, jobId, GenerationExecutionOwner.from(anchor));
    }

    /** Verifies the exact persisted publication owner's live backend token before any job lock. */
    @Override
    public void requirePublication(Connection connection, String tenantId, String jobId)
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
                requireCurrent(connection, tenantId, jobId, new GenerationExecutionOwner(
                        result.getString(1), result.getString(2), result.getLong(3)));
            }
        }
    }
}
