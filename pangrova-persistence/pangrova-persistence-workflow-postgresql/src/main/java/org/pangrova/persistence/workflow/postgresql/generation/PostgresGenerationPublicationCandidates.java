package org.pangrova.persistence.workflow.postgresql.generation;

import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.persistence.jdbc.api.generation.GenerationPublicationCandidates;
import org.pangrova.common.api.annotation.Internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Read-only Artifact/Query projection on their shared database; never an execution-ownership authority. */
@Internal
public final class PostgresGenerationPublicationCandidates implements GenerationPublicationCandidates {
    private static final String LIVE_PUBLICATION_ANCHOR =
            "EXISTS (SELECT 1 FROM artifact_execution_routes route "
                    + "JOIN execution_anchor anchor ON anchor.execution_id=route.execution_id "
                    + "WHERE route.tenant_id=artifact_build_jobs.tenant_id "
                    + "AND route.job_id=artifact_build_jobs.job_id "
                    + "AND anchor.owner_id=artifact_build_jobs.publication_execution_owner_id "
                    + "AND anchor.owner_incarnation=artifact_build_jobs.publication_execution_owner_incarnation "
                    + "AND anchor.fencing_token=artifact_build_jobs.publication_execution_owner_token "
                    + "AND anchor.expires_at>clock_timestamp())";

    @Override
    public List<GenerationJobKey> listReady(Connection connection, int limit, Instant now) throws SQLException {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Objects.requireNonNull(now, "now must not be null");
        try (PreparedStatement statement = connection.prepareStatement(
                     "SELECT tenant_id,job_id FROM artifact_build_jobs "
                             + "WHERE state='READY_TO_PUBLISH' AND "
                             + "(publication_lease_expires_at IS NULL "
                             + "OR publication_lease_expires_at<=? OR NOT " + LIVE_PUBLICATION_ANCHOR + ") "
                             + "ORDER BY updated_at,tenant_id,job_id LIMIT ?")) {
            setInstant(statement, 1, now);
            statement.setInt(2, limit);
            List<GenerationJobKey> jobs = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    jobs.add(new GenerationJobKey(
                            result.getString(1), result.getString(2)));
                }
            }
            return List.copyOf(jobs);
        }
    }

    @Override
    public boolean hasLiveOwner(Connection connection, String tenantId, String jobId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT " + LIVE_PUBLICATION_ANCHOR + " FROM artifact_build_jobs "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("artifact job disappeared during publication claim");
                }
                return result.getBoolean(1);
            }
        }
    }

}
