package org.pangrova.metadata.artifact.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** Cancels only the revoked generation, never a different subject's build of the same artifact. */
@org.pangrova.spi.annotation.Internal
public final class ArtifactBuildRevocation {
    private ArtifactBuildRevocation() {}

    public static boolean cancel(Connection connection, String tenantId, String jobId, String reason, Instant now)
            throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT job_id FROM artifact_build_jobs WHERE tenant_id=? AND job_id=? "
                        + "AND state IN ('QUEUED','RUNNING','READY_TO_PUBLISH') FOR UPDATE")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) return false;
            }
        }
        cancelLocked(connection, tenantId, jobId, reason, now);
        return true;
    }

    /** Locks one exact, ordered job set before touching pages; later inserts are not in this cancellation. */
    public static int cancelArtifact(Connection connection, String tenantId, String artifactId, Instant now)
            throws SQLException {
        List<String> jobs = new ArrayList<>();
        try (var statement = connection.prepareStatement(
                "SELECT job_id FROM artifact_build_jobs WHERE tenant_id=? AND artifact_id=? "
                        + "AND state IN ('QUEUED','RUNNING','READY_TO_PUBLISH') ORDER BY job_id FOR UPDATE")) {
            statement.setString(1, tenantId);
            statement.setString(2, artifactId);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) jobs.add(rows.getString(1));
            }
        }
        for (String jobId : jobs) cancelLocked(connection, tenantId, jobId, null, now);
        return jobs.size();
    }

    private static void cancelLocked(Connection connection, String tenantId, String jobId,
            String reason, Instant now) throws SQLException {
        try (var job = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='CANCELLED',failure_reason=COALESCE(?,failure_reason),updated_at=?,"
                        + "publication_lease_expires_at=NULL WHERE tenant_id=? AND job_id=? "
                        + "AND state IN ('QUEUED','RUNNING','READY_TO_PUBLISH')")) {
            job.setString(1, reason);
            setInstant(job, 2, now);
            job.setString(3, tenantId);
            job.setString(4, jobId);
            if (job.executeUpdate() != 1) throw new IllegalStateException("Locked artifact cancellation lost its job");
        }
        try (var pages = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='CANCELLED',lease_owner=NULL,lease_expires_at=NULL,updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND state IN ('PENDING','LEASED')")) {
            setInstant(pages, 1, now);
            pages.setString(2, tenantId);
            pages.setString(3, jobId);
            pages.executeUpdate();
        }
    }
}
