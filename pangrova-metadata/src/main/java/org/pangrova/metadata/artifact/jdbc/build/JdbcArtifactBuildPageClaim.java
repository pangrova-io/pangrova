package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.spi.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreMappings.fencingToken;
import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreSupport.failExhaustedPage;
import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreSupport.loadDependencies;
import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreSupport.loadSplits;
import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreSupport.markJobRunning;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** Claims one already-locked page in the current-authority transaction. */
final class JdbcArtifactBuildPageClaim {
    private JdbcArtifactBuildPageClaim() {}

    static Optional<ArtifactBuildLease> claim(Connection connection, JdbcArtifactBuildStoreMappings.PageRow page,
            ExecutionAnchor anchor, long ownerEpoch, Duration duration, Instant now) throws SQLException {
        int attemptId = Math.incrementExact(page.attemptId());
        if (attemptId >= page.maxAttempts()) {
            failExhaustedPage(connection, page, now);
            return Optional.empty();
        }
        long token = fencingToken(ownerEpoch, attemptId);
        Instant expiresAt = now.plus(duration);
        persist(connection, page, anchor, new IssuedLease(attemptId, token, ownerEpoch, expiresAt), now);
        markJobRunning(connection, page.tenantId(), page.jobId(), now);
        return Optional.of(new ArtifactBuildLease(page.tenantId(), page.jobId(), page.pageId(),
                page.stageId(), page.partitionId(), page.pageOrdinal(), page.kind(),
                loadSplits(connection, page.tenantId(), page.jobId(), page.pageId()),
                loadDependencies(connection, page.tenantId(), page.jobId(), page.pageId()),
                attemptId, token, anchor.ownerId(), ownerEpoch, expiresAt, anchor));
    }

    private static void persist(Connection connection, JdbcArtifactBuildStoreMappings.PageRow page,
            ExecutionAnchor anchor, IssuedLease lease, Instant now) throws SQLException {
        try (var statement = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='LEASED', attempt_id=?, "
                + "fencing_token=?, lease_owner=?, owner_epoch=?, lease_expires_at=?, "
                + "failure_reason=NULL, updated_at=?, "
                + "execution_owner_id=?, execution_owner_incarnation=?, execution_owner_token=? "
                + "WHERE tenant_id=? AND job_id=? AND page_id=? AND attempt_id=? AND state IN ('PENDING','LEASED')")) {
            int parameter = 1;
            statement.setInt(parameter++, lease.attemptId());
            statement.setLong(parameter++, lease.token());
            statement.setString(parameter++, anchor.ownerId());
            statement.setLong(parameter++, lease.ownerEpoch());
            setInstant(statement, parameter++, lease.expiresAt());
            setInstant(statement, parameter++, now);
            statement.setString(parameter++, anchor.ownerId());
            statement.setString(parameter++, anchor.ownerIncarnation());
            statement.setLong(parameter++, anchor.fencingToken());
            statement.setString(parameter++, page.tenantId());
            statement.setString(parameter++, page.jobId());
            statement.setString(parameter++, page.pageId());
            statement.setInt(parameter, page.attemptId());
            if (statement.executeUpdate() != 1) throw new IllegalStateException("artifact page lease fence was lost");
        }
    }

    private record IssuedLease(int attemptId, long token, long ownerEpoch, Instant expiresAt) {}
}
