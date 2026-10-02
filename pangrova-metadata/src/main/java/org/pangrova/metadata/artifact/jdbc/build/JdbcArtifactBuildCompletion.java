package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.jdbc.ArtifactCurrentAuthority;
import org.pangrova.metadata.artifact.jdbc.ArtifactExecutionFence;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.metadata.artifact.build.ArtifactPageCompletion;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** Applies one fenced artifact page completion inside its caller's transaction. */
final class JdbcArtifactBuildCompletion {

    private JdbcArtifactBuildCompletion() {}

    static boolean complete(Connection connection, ArtifactPageCompletion completion)
            throws SQLException {
        ArtifactCurrentAuthority.requireCurrent(connection, completion.tenantId(), completion.jobId());
        try {
            ArtifactExecutionFence.requireCurrent(connection, completion.tenantId(), completion.jobId(), completion.executionOwner());
        } catch (ArtifactExecutionFence.LostOwnerException staleOwner) {
            return false;
        }
        ArtifactBuildJob job = JdbcArtifactBuildStoreMappings.findJob(
                connection, completion.tenantId(), completion.jobId(), true).orElse(null);
        if (job == null) {
            return false;
        }
        JdbcArtifactBuildStoreMappings.PageRow page = JdbcArtifactBuildPageSelection.findPage(
                connection, completion.tenantId(), completion.jobId(),
                completion.pageId(), true).orElse(null);
        if (page == null) {
            return false;
        }
        if (!completion.executionOwner().equals(page.executionOwner())) {
            return false;
        }
        if ("SUCCEEDED".equals(page.state())) {
            return sameReport(page, completion);
        }
        if (!hasLiveFence(page, completion)) {
            return false;
        }
        validateAndInsertOutputs(connection, job, page, completion);
        markSucceeded(connection, completion);
        JdbcArtifactBuildStoreSupport.advanceCompletionBarrier(
                connection, completion.tenantId(),
                completion.jobId(), completion.completedAt());
        return true;
    }

    private static boolean sameReport(
            JdbcArtifactBuildStoreMappings.PageRow page,
            ArtifactPageCompletion completion) {
        return completion.reportSha256().equalsIgnoreCase(page.reportSha256())
                && MessageDigest.isEqual(completion.reportBytes(), page.reportBytes());
    }

    private static boolean hasLiveFence(
            JdbcArtifactBuildStoreMappings.PageRow page,
            ArtifactPageCompletion completion) {
        return "LEASED".equals(page.state())
                && page.fencingToken() == completion.fencingToken()
                && page.ownerEpoch() == completion.ownerEpoch()
                && page.leaseExpiresAt() != null
                && !page.leaseExpiresAt().isBefore(completion.completedAt());
    }

    private static void validateAndInsertOutputs(
            Connection connection, ArtifactBuildJob job,
            JdbcArtifactBuildStoreMappings.PageRow page,
            ArtifactPageCompletion completion) throws SQLException {
        JdbcArtifactBuildStoreSupport.validateOutputCoverage(
                job, page,
                JdbcArtifactBuildStoreSupport.loadSplits(
                        connection, completion.tenantId(),
                        completion.jobId(), completion.pageId()).stream()
                        .map(ArtifactBuildSplit::dataObjectId).toList(),
                completion.outputs());
        for (ArtifactBuildOutput output : completion.outputs()) {
            JdbcArtifactBuildStoreSupport.insertOutput(
                    connection, completion.tenantId(),
                    completion.jobId(), completion.pageId(), output);
        }
    }

    private static void markSucceeded(
            Connection connection, ArtifactPageCompletion completion) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='SUCCEEDED', report_bytes=?, "
                        + "report_sha256=?, lease_owner=NULL, lease_expires_at=NULL, "
                        + "failure_reason=NULL, updated_at=? WHERE job_id=? AND page_id=? "
                        + "AND tenant_id=? AND state='LEASED' "
                        + "AND fencing_token=? AND owner_epoch=?")) {
            statement.setBytes(1, completion.reportBytes());
            statement.setString(2, completion.reportSha256()
                    .toLowerCase(java.util.Locale.ROOT));
            setInstant(statement, 3, completion.completedAt());
            statement.setString(4, completion.jobId());
            statement.setString(5, completion.pageId());
            statement.setString(6, completion.tenantId());
            statement.setLong(7, completion.fencingToken());
            statement.setLong(8, completion.ownerEpoch());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("artifact completion fence was lost");
            }
        }
    }
}
