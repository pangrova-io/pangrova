package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;

import org.pangrova.generation.api.exception.GenerationExecutionOwnerLostException;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;

import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.page.GenerationPageCompletion;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Applies one fenced artifact page completion inside its caller's transaction. */
final class PostgresGenerationCompletion {
    private static final PostgresGenerationQueries INPUTS = new PostgresGenerationQueries();

    private PostgresGenerationCompletion() {}

    static boolean complete(GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence,
            Connection connection, GenerationPageCompletion completion)
            throws SQLException {
        transactionAuthority.requireCurrent(connection, completion.tenantId(), completion.jobId());
        if (!ownsExecution(executionFence, connection, completion)) return false;
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
        return applyCompletion(connection, completion, job, page);
    }

    /** Producer authorization and the exact job and page locks are held by the caller's transaction. */
    static boolean completeLockedProducer(GenerationExecutionTransactionFence executionFence, Connection connection,
            GenerationPageCompletion completion, GenerationJob job,
            PostgresGenerationJobStoreMappings.PageRow page) throws SQLException {
        // Recheck the execution deadline after the potentially blocking job, page and exchange locks.
        if (!ownsExecution(executionFence, connection, completion)) return false;
        return applyCompletion(connection, completion, job, page);
    }

    private static boolean ownsExecution(GenerationExecutionTransactionFence executionFence,
            Connection connection, GenerationPageCompletion completion) throws SQLException {
        try {
            executionFence.requireCurrent(connection, completion.tenantId(),
                    completion.jobId(), completion.executionOwner());
            return true;
        } catch (GenerationExecutionOwnerLostException staleOwner) {
            return false;
        }
    }

    private static boolean applyCompletion(Connection connection, GenerationPageCompletion completion,
            GenerationJob job, PostgresGenerationJobStoreMappings.PageRow page) throws SQLException {
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
        PostgresGenerationJobStoreSupport.advanceCompletionBarrier(connection, job, completion.completedAt());
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
        List<String> expectedSources = page.kind() == GenerationPageKind.RELATIONAL_COMPUTE ? List.of()
                : INPUTS.loadSplits(connection, completion.tenantId(), completion.jobId(), completion.pageId())
                        .stream().map(GenerationSplit::dataObjectId).toList();
        PostgresGenerationJobStoreSupport.validateOutputCoverage(job, page, expectedSources, completion.outputs());
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
