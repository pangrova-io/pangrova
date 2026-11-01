package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.ArtifactBuildJob;
import org.pangrova.indexing.control.ArtifactBuildJobState;
import org.pangrova.indexing.control.ArtifactBuildJobKey;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.indexing.store.jdbc.JdbcIndexSupport.setInstant;

/**
 * Publication-claim persistence for the durable artifact-build state machine.
 * Isolated from {@link JdbcArtifactBuildStore} so each class stays within the
 * repository size gate while the public facade keeps owning transactions.
 */
final class JdbcArtifactPublicationClaims {

    private static final String LIVE_PUBLICATION_ANCHOR =
            "EXISTS (SELECT 1 FROM artifact_execution_routes route "
                    + "JOIN execution_anchor anchor ON anchor.execution_id=route.execution_id "
                    + "WHERE route.tenant_id=artifact_build_jobs.tenant_id "
                    + "AND route.job_id=artifact_build_jobs.job_id "
                    + "AND anchor.owner_id=artifact_build_jobs.publication_execution_owner_id "
                    + "AND anchor.owner_incarnation=artifact_build_jobs.publication_execution_owner_incarnation "
                    + "AND anchor.fencing_token=artifact_build_jobs.publication_execution_owner_token "
                    + "AND anchor.expires_at>clock_timestamp())";

    private JdbcArtifactPublicationClaims() {
    }

    static List<ArtifactBuildJobKey> listReadyForPublication(
            DataSource dataSource, int limit, Instant now) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Objects.requireNonNull(now, "now must not be null");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT tenant_id,job_id FROM artifact_build_jobs "
                             + "WHERE state='READY_TO_PUBLISH' AND "
                             + "(publication_lease_expires_at IS NULL "
                             + "OR publication_lease_expires_at<=? OR NOT " + LIVE_PUBLICATION_ANCHOR + ") "
                             + "ORDER BY updated_at,tenant_id,job_id LIMIT ?")) {
            setInstant(statement, 1, now);
            statement.setInt(2, limit);
            List<ArtifactBuildJobKey> jobs = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    jobs.add(new ArtifactBuildJobKey(
                            result.getString(1), result.getString(2)));
                }
            }
            return List.copyOf(jobs);
        } catch (SQLException e) {
            throw JdbcArtifactBuildStoreMappings.failure("list publishable artifact jobs", e);
        }
    }

    static Optional<ArtifactBuildJob> claimReadyForPublication(
            Connection connection, ArtifactCurrentAuthority authority,
            String tenantId, String jobId,
            org.pangrova.spi.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now) throws SQLException {
        String owner = executionAnchor.ownerId();
        authority.requireCurrent(connection, tenantId, jobId);
        ArtifactExecutionFence.requireAnchor(connection, tenantId, jobId, executionAnchor);
        ArtifactBuildJob job = JdbcArtifactBuildStoreMappings.findJob(connection, tenantId, jobId, true).orElse(null);
        if (job == null || job.state() != ArtifactBuildJobState.READY_TO_PUBLISH) {
            return Optional.empty();
        }
        JdbcArtifactBuildStoreMappings.PublicationLease current =
                JdbcArtifactBuildStoreMappings.loadPublicationLease(connection, tenantId, jobId);
        if (current.expiresAt() != null && current.expiresAt().isAfter(now)
                && (!owner.equals(current.owner()) || ownerEpoch != current.ownerEpoch())
                && hasLivePublicationAnchor(connection, tenantId, jobId)) {
            return Optional.empty();
        }
        long nextFence = Math.addExact(job.publicationFencingToken(), 1L);
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET publication_owner=?, "
                        + "publication_owner_epoch=?, publication_fencing_token=?, "
                        + "publication_lease_expires_at=?, updated_at=?, "
                        + "publication_execution_owner_id=?, publication_execution_owner_incarnation=?, "
                        + "publication_execution_owner_token=? "
                        + "WHERE tenant_id=? AND job_id=? AND state='READY_TO_PUBLISH' "
                        + "AND publication_fencing_token=?")) {
            int parameter = 1;
            statement.setString(parameter++, owner);
            statement.setLong(parameter++, ownerEpoch);
            statement.setLong(parameter++, nextFence);
            setInstant(statement, parameter++, now.plus(leaseDuration));
            setInstant(statement, parameter++, now);
            statement.setString(parameter++, executionAnchor.ownerId());
            statement.setString(parameter++, executionAnchor.ownerIncarnation());
            statement.setLong(parameter++, executionAnchor.fencingToken());
            statement.setString(parameter++, tenantId);
            statement.setString(parameter++, jobId);
            statement.setLong(parameter, job.publicationFencingToken());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("artifact publication fence was lost");
            }
        }
        return JdbcArtifactBuildStoreMappings.findJob(connection, tenantId, jobId, false);
    }

    private static boolean hasLivePublicationAnchor(
            Connection connection, String tenantId, String jobId) throws SQLException {
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

    static boolean markPublished(
            Connection connection, ArtifactCurrentAuthority authority,
            String tenantId, String jobId, String owner, long ownerEpoch,
            long publicationFencingToken, Instant now) throws SQLException {
        authority.requireCurrent(connection, tenantId, jobId);
        var candidate = JdbcArtifactBuildStoreMappings.findJob(connection, tenantId, jobId, false);
        if (candidate.isEmpty()
                || candidate.orElseThrow().state() != ArtifactBuildJobState.READY_TO_PUBLISH) {
            return false;
        }
        ArtifactExecutionFence.requirePublication(connection, tenantId, jobId);
        try (var statement = connection.prepareStatement(
            "UPDATE artifact_build_jobs SET state='PUBLISHED', "
                    + "publication_lease_expires_at=NULL, failure_reason=NULL, updated_at=? "
                    + "WHERE tenant_id=? AND job_id=? AND state='READY_TO_PUBLISH' "
                    + "AND publication_owner=? AND publication_owner_epoch=? "
                    + "AND publication_fencing_token=? "
                    + "AND publication_lease_expires_at>=?")) {
            setInstant(statement, 1, now);
            statement.setString(2, tenantId);
            statement.setString(3, jobId);
            statement.setString(4, owner);
            statement.setLong(5, ownerEpoch);
            statement.setLong(6, publicationFencingToken);
            setInstant(statement, 7, now);
            return statement.executeUpdate() == 1;
        }
    }

    static boolean deferPublication(
            Connection connection, ArtifactCurrentAuthority authority,
            ArtifactBuildJob claim, String owner, long ownerEpoch,
            Instant nextAvailableAt, String failureReason, Instant now) throws SQLException {
        String tenantId = claim.tenantId();
        String jobId = claim.jobId();
        long publicationFencingToken = claim.publicationFencingToken();
        String normalizedFailure =
                JdbcArtifactBuildStoreMappings.normalizeFailure(failureReason);
        authority.requireCurrent(connection, tenantId, jobId);
        ArtifactExecutionFence.requirePublication(connection, tenantId, jobId);
        try (var statement = connection.prepareStatement(
            "UPDATE artifact_build_jobs SET publication_lease_expires_at=?, "
                    + "failure_reason=?, updated_at=? "
                    + "WHERE tenant_id=? AND job_id=? AND state='READY_TO_PUBLISH' "
                    + "AND publication_owner=? AND publication_owner_epoch=? "
                    + "AND publication_fencing_token=?")) {
            setInstant(statement, 1, nextAvailableAt);
            statement.setString(2, normalizedFailure);
            setInstant(statement, 3, now);
            statement.setString(4, tenantId);
            statement.setString(5, jobId);
            statement.setString(6, owner);
            statement.setLong(7, ownerEpoch);
            statement.setLong(8, publicationFencingToken);
            return statement.executeUpdate() == 1;
        }
    }
}
