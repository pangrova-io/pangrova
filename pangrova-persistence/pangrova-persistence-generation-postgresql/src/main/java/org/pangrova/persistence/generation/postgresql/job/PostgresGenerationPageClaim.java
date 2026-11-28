package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;
import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries.PageInputs;

import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreMappings.fencingToken;
import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreSupport.failExhaustedPage;
import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreSupport.markJobRunning;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Claims one already-locked page in the current-authority transaction. */
final class PostgresGenerationPageClaim {
    private static final PostgresGenerationQueries INPUTS = new PostgresGenerationQueries();

    private PostgresGenerationPageClaim() {}

    static Optional<GenerationLease> claim(Connection connection, PostgresGenerationJobStoreMappings.PageRow page,
            ExecutionAnchor anchor, long ownerEpoch, Duration duration, Instant now, boolean readsSplits)
            throws SQLException {
        if (attemptsExhausted(page)) {
            failExhaustedPage(connection, page, now);
            return Optional.empty();
        }
        var inputs = readInputs(connection, page, readsSplits);
        var lease = issue(connection, page, inputs, anchor, ownerEpoch, duration, now);
        markJobRunning(connection, page.tenantId(), page.jobId(), now);
        return Optional.of(lease);
    }

    static PageInputs readInputs(Connection connection, PostgresGenerationJobStoreMappings.PageRow page,
            boolean readsSplits) throws SQLException {
        var splits = readsSplits
                ? INPUTS.loadSplits(connection, page.tenantId(), page.jobId(), page.pageId())
                : List.<GenerationSplit>of();
        var dependencies = page.kind().isRelational() ? List.<String>of()
                : INPUTS.loadDependencies(connection, page.tenantId(), page.jobId(), page.pageId());
        return new PageInputs(splits, dependencies);
    }

    static boolean attemptsExhausted(PostgresGenerationJobStoreMappings.PageRow page) {
        return Math.incrementExact(page.attemptId()) >= page.maxAttempts();
    }

    /** Issues one locked page whose attempt budget was validated before the first cohort mutation. */
    static GenerationLease issue(Connection connection, PostgresGenerationJobStoreMappings.PageRow page,
            PageInputs inputs, ExecutionAnchor anchor, long ownerEpoch, Duration duration, Instant now)
            throws SQLException {
        int attemptId = Math.incrementExact(page.attemptId());
        long token = fencingToken(ownerEpoch, attemptId);
        Instant expiresAt = persist(connection, page, anchor,
                new IssuedLease(attemptId, token, ownerEpoch, now.plus(duration)), now);
        return new GenerationLease(page.tenantId(), page.jobId(), page.pageId(),
                page.stageId(), page.partitionId(), page.pageOrdinal(), page.kind(),
                inputs.splits(), inputs.dependencies(),
                attemptId, token, anchor.ownerId(), ownerEpoch, expiresAt, anchor);
    }

    private static Instant persist(Connection connection, PostgresGenerationJobStoreMappings.PageRow page,
            ExecutionAnchor anchor, IssuedLease lease, Instant now) throws SQLException {
        try (var statement = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='LEASED', attempt_id=?, "
                + "fencing_token=?, lease_owner=?, owner_epoch=?, lease_expires_at=?, "
                + "failure_reason=NULL, updated_at=?, "
                + "execution_owner_id=?, execution_owner_incarnation=?, execution_owner_token=? "
                + "WHERE tenant_id=? AND job_id=? AND page_id=? AND attempt_id=? AND state IN ('PENDING','LEASED') "
                + "RETURNING lease_expires_at")) {
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
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("artifact page lease fence was lost");
                // Use the stored microsecond timestamp as the lease's exact dispatch deadline.
                return getInstant(result, "lease_expires_at");
            }
        }
    }

    private record IssuedLease(int attemptId, long token, long ownerEpoch, Instant expiresAt) {}
}
