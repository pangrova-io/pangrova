package org.pangrova.persistence.jdbc.generation.job;

import org.pangrova.persistence.jdbc.generation.query.PostgresGenerationQueries;

import org.pangrova.spi.generation.execution.GenerationExecutionOwnerLostException;

import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationTransactionAuthority;
import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationExecutionTransactionFence;

import org.pangrova.spi.generation.job.GenerationJob;
import org.pangrova.spi.generation.job.GenerationOutput;
import org.pangrova.spi.generation.job.GenerationSplit;
import org.pangrova.spi.generation.job.GenerationPageCompletion;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/** Applies one fenced artifact page completion inside its caller's transaction. */
final class PostgresGenerationCompletion {
    private static final PostgresGenerationQueries INPUTS = new PostgresGenerationQueries();

    private PostgresGenerationCompletion() {}

    static boolean complete(GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence,
            Connection connection, GenerationPageCompletion completion)
            throws SQLException {
        transactionAuthority.requireCurrent(connection, completion.tenantId(), completion.jobId());
        try {
            executionFence.requireCurrent(connection, completion.tenantId(),
                    completion.jobId(), completion.executionOwner());
        } catch (GenerationExecutionOwnerLostException staleOwner) {
            return false;
        }
        GenerationJob job = PostgresGenerationJobStoreMappings.findJob(
                connection, completion.tenantId(), completion.jobId(), true).orElse(null);
        if (job == null) {
            return false;
        }
        PostgresGenerationJobStoreMappings.PageRow page = PostgresGenerationPageSelection.findPage(
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
        PostgresGenerationJobStoreSupport.advanceCompletionBarrier(
                connection, completion.tenantId(),
                completion.jobId(), completion.completedAt());
        return true;
    }

    private static boolean sameReport(
            PostgresGenerationJobStoreMappings.PageRow page,
            GenerationPageCompletion completion) {
        return completion.reportSha256().equalsIgnoreCase(page.reportSha256())
                && MessageDigest.isEqual(completion.reportBytes(), page.reportBytes());
    }

    private static boolean hasLiveFence(
            PostgresGenerationJobStoreMappings.PageRow page,
            GenerationPageCompletion completion) {
        return "LEASED".equals(page.state())
                && page.fencingToken() == completion.fencingToken()
                && page.ownerEpoch() == completion.ownerEpoch()
                && page.leaseExpiresAt() != null
                && !page.leaseExpiresAt().isBefore(completion.completedAt());
    }

    private static void validateAndInsertOutputs(
            Connection connection, GenerationJob job,
            PostgresGenerationJobStoreMappings.PageRow page,
            GenerationPageCompletion completion) throws SQLException {
        PostgresGenerationJobStoreSupport.validateOutputCoverage(
                job, page,
                INPUTS.loadSplits(
                        connection, completion.tenantId(),
                        completion.jobId(), completion.pageId()).stream()
                        .map(GenerationSplit::dataObjectId).toList(),
                completion.outputs());
        for (GenerationOutput output : completion.outputs()) {
            PostgresGenerationJobStoreSupport.insertOutput(
                    connection, completion.tenantId(),
                    completion.jobId(), completion.pageId(), output);
        }
    }

    private static void markSucceeded(
            Connection connection, GenerationPageCompletion completion) throws SQLException {
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
