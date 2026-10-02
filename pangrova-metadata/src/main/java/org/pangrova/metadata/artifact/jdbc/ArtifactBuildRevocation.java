package org.pangrova.metadata.artifact.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** Cancels only the revoked generation, never a different subject's build of the same artifact. */
@org.pangrova.spi.annotation.Internal
public final class ArtifactBuildRevocation {
    private ArtifactBuildRevocation() {}

    public static boolean cancel(Connection connection, String tenantId, String jobId, String reason, Instant now)
            throws SQLException {
        try (var job = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='CANCELLED',failure_reason=?,updated_at=?,"
                        + "publication_lease_expires_at=NULL WHERE tenant_id=? AND job_id=? "
                        + "AND state IN ('QUEUED','RUNNING','READY_TO_PUBLISH')")) {
            job.setString(1, reason);
            setInstant(job, 2, now);
            job.setString(3, tenantId);
            job.setString(4, jobId);
            if (job.executeUpdate() != 1) {
                return false;
            }
        }
        try (var pages = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='CANCELLED',lease_expires_at=NULL,updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND state IN ('PENDING','LEASED')")) {
            setInstant(pages, 1, now);
            pages.setString(2, tenantId);
            pages.setString(3, jobId);
            pages.executeUpdate();
        }
        return true;
    }
}
