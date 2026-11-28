package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.InputRange;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.NativeWinner;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PartitionClaim;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreMappings.PageRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** One page-selection contract for ordinary polling and exact durable stage partitions. */
final class PostgresGenerationPageSelection {
    private PostgresGenerationPageSelection() {}

    private static final String FINALIZED_STAGE_INPUTS = """
            AND NOT EXISTS (
                SELECT 1 FROM artifact_stage_exchanges e
                WHERE e.tenant_id = p.tenant_id AND e.job_id = p.job_id AND e.consumer_stage_id = p.stage_id
                    AND NOT EXISTS (
                        SELECT 1 FROM artifact_exchange_finalizations f
                        WHERE f.tenant_id = e.tenant_id AND f.job_id = e.job_id AND f.exchange_id = e.exchange_id
                    )
            )
            """;

    // The mixed job queue includes artifact reductions with page-level dependencies.
    static final String READY_PAGE_INPUTS = """
            AND (p.page_kind IN ('RELATIONAL_COMPUTE', 'RELATIONAL_PARTITION') OR NOT EXISTS (
                SELECT 1 FROM artifact_build_page_dependencies d
                JOIN artifact_build_pages u ON u.tenant_id = d.tenant_id
                    AND u.job_id = d.job_id AND u.page_id = d.dependency_page_id
                WHERE d.tenant_id = p.tenant_id AND d.job_id = p.job_id AND d.page_id = p.page_id
                    AND u.state <> 'SUCCEEDED'
            ))
            """ + FINALIZED_STAGE_INPUTS;

    private static final String CLAIMABLE_PAGE = """
            SELECT p.* FROM artifact_build_pages p WHERE p.tenant_id=? AND p.job_id=?
            AND ((p.state='PENDING' AND p.next_available_at<=?) OR (p.state='LEASED' AND p.lease_expires_at<?))
            """;
    private static final String SELECT_NEXT = "ORDER BY page_ordinal LIMIT 1 FOR UPDATE SKIP LOCKED";
    private static final String CLAIMABLE_ANY = CLAIMABLE_PAGE + READY_PAGE_INPUTS + SELECT_NEXT;
    private static final String CLAIMABLE_RELATIONAL = CLAIMABLE_PAGE + FINALIZED_STAGE_INPUTS + SELECT_NEXT;
    private static final String SELECT_COHORT = """
            WITH requested(partition_id, page_ordinal) AS (
                SELECT * FROM unnest(?::integer[], ?::integer[])
            ),
            exact AS MATERIALIZED (
                SELECT p.partition_id, p.page_id FROM artifact_build_pages p
                JOIN requested r USING (partition_id, page_ordinal)
                WHERE tenant_id=? AND job_id=? AND stage_id=?
            ),
            claimable AS (
            %s
            AND page_id IN (SELECT page_id FROM exact)
            ORDER BY partition_id FOR UPDATE SKIP LOCKED
            )
            SELECT (SELECT count(*) FROM exact WHERE partition_id=r.partition_id) AS allocations,
                   claimable.*
            FROM requested r LEFT JOIN claimable USING (partition_id)
            ORDER BY r.partition_id
            """;
    private static final String CLAIMABLE_COHORT = SELECT_COHORT.formatted(CLAIMABLE_PAGE + READY_PAGE_INPUTS);
    private static final String RELATIONAL_COHORT = SELECT_COHORT.formatted(CLAIMABLE_PAGE + FINALIZED_STAGE_INPUTS);

    static Optional<PageRow> selectClaimablePage(Connection connection, String tenantId, String jobId, Instant now,
            boolean relational) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                relational ? CLAIMABLE_RELATIONAL : CLAIMABLE_ANY)) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            setInstant(statement, 3, now);
            setInstant(statement, 4, now);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapPage(result)) : Optional.empty();
            }
        }
    }

    /** Locks pages in partition order and distinguishes a missing identity from an unavailable page. */
    static List<PageRow> selectClaimableCohort(Connection connection, List<PartitionClaim> cohort,
            boolean relational) throws SQLException {
        var first = cohort.get(0);
        try (var statement = connection.prepareStatement(relational ? RELATIONAL_COHORT : CLAIMABLE_COHORT)) {
            statement.setObject(1, cohort.stream().mapToInt(PartitionClaim::partitionId).toArray());
            statement.setObject(2, cohort.stream().mapToInt(PartitionClaim::pageOrdinal).toArray());
            statement.setString(3, first.job().tenantId());
            statement.setString(4, first.job().jobId());
            statement.setInt(5, first.stageId());
            statement.setString(6, first.job().tenantId());
            statement.setString(7, first.job().jobId());
            setInstant(statement, 8, first.now());
            setInstant(statement, 9, first.now());

            var selected = new ArrayList<PageRow>(cohort.size());
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    if (rows.getLong("allocations") != 1) {
                        throw new IllegalStateException(
                                "Durable stage partition lacks one exact immutable page allocation");
                    }
                    if (rows.getString("page_id") != null) selected.add(mapPage(rows));
                }
            }
            return selected.size() == cohort.size() ? selected : List.of();
        }
    }

    static Optional<PageRow> findPage(
            Connection connection, String tenantId, String jobId,
            String pageId, boolean lock) throws SQLException {
        String sql = "SELECT * FROM artifact_build_pages "
                + "WHERE tenant_id=? AND job_id=? AND page_id=?"
                + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setString(3, pageId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapPage(result)) : Optional.empty();
            }
        }
    }

    static Optional<PageRow> findPartition(Connection connection, String tenantId, String jobId,
            int stageId, int partitionId) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM artifact_build_pages "
                + "WHERE tenant_id=? AND job_id=? AND stage_id=? AND partition_id=? FOR UPDATE")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setInt(3, stageId);
            statement.setInt(4, partitionId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                var page = mapPage(result);
                if (result.next()) throw new IllegalStateException("Durable partition has ambiguous page identity");
                return Optional.of(page);
            }
        }
    }

    static NativeWinner nativeWinner(PageRow page) {
        return new NativeWinner(page.pageId(), page.attemptId(), page.fencingToken(), page.ownerEpoch(),
                page.executionOwner(), page.reportBytes(), page.reportSha256());
    }

    static List<PageRow> lockWindow(Connection connection, String tenantId, String jobId,
            int stageId, GenerationPageKind kind, int firstPartition, int count) throws SQLException {
        var pages = new ArrayList<PageRow>(count);
        try (var statement = connection.prepareStatement("""
                SELECT * FROM artifact_build_pages
                WHERE tenant_id=? AND job_id=? AND stage_id=? AND partition_id>=? AND partition_id<?
                ORDER BY partition_id FOR UPDATE
                """)) {
            statement.setFetchSize(4);
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setInt(3, stageId);
            statement.setInt(4, firstPartition);
            statement.setInt(5, firstPartition + count);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    var page = mapPage(rows);
                    if (page.partitionId() != firstPartition + pages.size() || page.kind() != kind) {
                        throw new IllegalStateException("Persisted pages differ from their exact stage partitions");
                    }
                    pages.add(page);
                }
            }
        }
        if (pages.size() != count) {
            throw new IllegalStateException("Persisted stage partitions are missing from the window");
        }
        return List.copyOf(pages);
    }

    static PageRow mapPage(ResultSet result) throws SQLException {
        Instant leaseExpiresAt = getInstant(result, "lease_expires_at");
        int inputEnd = result.getInt("exchange_partition_end");
        var range = inputEnd == 0 ? null : new InputRange(result.getInt("exchange_partition_start"), inputEnd);
        return new PageRow(
                result.getString("tenant_id"), result.getString("job_id"),
                result.getString("page_id"),
                result.getInt("stage_id"), result.getInt("partition_id"),
                result.getInt("page_ordinal"),
                GenerationPageKind.valueOf(result.getString("page_kind")),
                result.getString("state"),
                result.getInt("attempt_id"), result.getLong("fencing_token"),
                result.getString("lease_owner"), result.getLong("owner_epoch"),
                leaseExpiresAt, getInstant(result, "next_available_at"),
                result.getInt("max_attempts"), result.getBytes("report_bytes"),
                result.getString("report_sha256"), range,
                result.getString("execution_owner_id") == null ? null
                        : new org.pangrova.generation.api.execution.GenerationExecutionOwner(
                                result.getString("execution_owner_id"), result.getString("execution_owner_incarnation"),
                                result.getLong("execution_owner_token")));
    }

}
