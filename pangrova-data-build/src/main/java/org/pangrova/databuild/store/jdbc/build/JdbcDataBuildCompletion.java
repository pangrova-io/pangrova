package org.pangrova.databuild.store.jdbc.build;

import org.pangrova.databuild.api.execution.DataBuildExecutionOwnerLostException;

import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;
import org.pangrova.databuild.spi.jdbc.DataBuildExecutionTransactionFence;

import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.build.DataBuildPageCompletion;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/** Applies one fenced artifact page completion inside its caller's transaction. */
final class JdbcDataBuildCompletion {

    private JdbcDataBuildCompletion() {}

    static boolean complete(DataBuildTransactionAuthority transactionAuthority,
            DataBuildExecutionTransactionFence executionFence,
            Connection connection, DataBuildPageCompletion completion)
            throws SQLException {
        transactionAuthority.requireCurrent(connection, completion.tenantId(), completion.jobId());
        try {
            executionFence.requireCurrent(connection, completion.tenantId(),
                    completion.jobId(), completion.executionOwner());
        } catch (DataBuildExecutionOwnerLostException staleOwner) {
            return false;
        }
        DataBuildJob job = JdbcDataBuildStoreMappings.findJob(
                connection, completion.tenantId(), completion.jobId(), true).orElse(null);
        if (job == null) {
            return false;
        }
        JdbcDataBuildStoreMappings.PageRow page = JdbcDataBuildPageSelection.findPage(
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
        JdbcDataBuildStoreSupport.advanceCompletionBarrier(
                connection, completion.tenantId(),
                completion.jobId(), completion.completedAt());
        return true;
    }

    private static boolean sameReport(
            JdbcDataBuildStoreMappings.PageRow page,
            DataBuildPageCompletion completion) {
        return completion.reportSha256().equalsIgnoreCase(page.reportSha256())
                && MessageDigest.isEqual(completion.reportBytes(), page.reportBytes());
    }

    private static boolean hasLiveFence(
            JdbcDataBuildStoreMappings.PageRow page,
            DataBuildPageCompletion completion) {
        return "LEASED".equals(page.state())
                && page.fencingToken() == completion.fencingToken()
                && page.ownerEpoch() == completion.ownerEpoch()
                && page.leaseExpiresAt() != null
                && !page.leaseExpiresAt().isBefore(completion.completedAt());
    }

    private static void validateAndInsertOutputs(
            Connection connection, DataBuildJob job,
            JdbcDataBuildStoreMappings.PageRow page,
            DataBuildPageCompletion completion) throws SQLException {
        JdbcDataBuildStoreSupport.validateOutputCoverage(
                job, page,
                org.pangrova.databuild.store.jdbc.query.JdbcDataBuildQueries.loadSplits(
                        connection, completion.tenantId(),
                        completion.jobId(), completion.pageId()).stream()
                        .map(DataBuildSplit::dataObjectId).toList(),
                completion.outputs());
        for (DataBuildOutput output : completion.outputs()) {
            JdbcDataBuildStoreSupport.insertOutput(
                    connection, completion.tenantId(),
                    completion.jobId(), completion.pageId(), output);
        }
    }

    private static void markSucceeded(
            Connection connection, DataBuildPageCompletion completion) throws SQLException {
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
