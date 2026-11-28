package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.enums.GenerationExchangeDistribution;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.InputRange;
import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreSupport.findJob;
import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreSupport.terminal;

/** Authoritative physical layout for a relational compute stage, frozen before output allocation. */
final class PostgresGenerationInputRanges {
    private PostgresGenerationInputRanges() {}

    static List<InputRange> freeze(Connection connection, PostgresCurrentGenerationAuthority authority,
            GenerationJobKey job, int stageId, List<InputRange> proposed) throws SQLException {
        var graph = authority.dispatchSpec().stageGraph();
        var stage = graph.requireStage(stageId);
        var inputs = graph.exchanges().stream().filter(edge -> edge.downstreamStageId() == stageId).toList();
        var outputs = graph.exchanges().stream().filter(edge -> edge.upstreamStageId() == stageId).toList();
        if (!stage.coalescibleHashInputs() || stage.pageKind() != GenerationPageKind.RELATIONAL_COMPUTE
                || inputs.isEmpty() || inputs.size() > 2 || outputs.size() != 1
                || outputs.get(0).distribution() == GenerationExchangeDistribution.BROADCAST
                || inputs.stream().anyMatch(edge -> edge.distribution() != GenerationExchangeDistribution.HASH
                        || edge.partitionCount() != stage.parallelism())) {
            throw new IllegalArgumentException("Stage cannot coalesce the requested durable Shuffle inputs");
        }
        String exchange = outputs.get(0).exchangeId();
        try (var statement = connection.prepareStatement("""
                SELECT e.physical_producer_count, EXISTS (
                    SELECT 1 FROM artifact_exchange_registrations r
                    WHERE r.tenant_id=e.tenant_id AND r.job_id=e.job_id AND r.exchange_id=e.exchange_id
                ) AS registered
                FROM artifact_stage_exchanges e
                WHERE e.tenant_id=? AND e.job_id=? AND e.exchange_id=? AND e.producer_stage_id=?
                FOR UPDATE OF e
                """)) {
            bindJob(statement, 1, job);
            statement.setString(3, exchange);
            statement.setInt(4, stageId);
            try (var row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("Durable layout is missing its outgoing edge");
                int frozen = row.getInt("physical_producer_count");
                if (frozen != 0) {
                    var ranges = readFrozen(connection, job, stageId, frozen);
                    requireCoverage(ranges, stage.parallelism());
                    return ranges;
                }
                if (row.getBoolean("registered") || proposed.isEmpty()) return List.of();
            }
        }
        requireCoverage(proposed, stage.parallelism());
        var current = findJob(connection, job.tenantId(), job.jobId(), true).orElseThrow();
        if (terminal(current.state()) || !hasFinalizedInputs(connection, job, stageId, inputs.size())) {
            throw new IllegalStateException("Durable layout requires finalized inputs and a runnable job");
        }
        if (!setPhysicalCount(connection, job, exchange, proposed.size())) return List.of();
        freezePages(connection, job, stageId, stage.parallelism(), proposed);
        try (var statement = connection.prepareStatement("""
                UPDATE artifact_build_jobs SET completed_pages=completed_pages+?
                WHERE tenant_id=? AND job_id=? AND completed_pages+? < expected_pages
                """)) {
            int retired = stage.parallelism() - proposed.size();
            statement.setInt(1, retired);
            bindJob(statement, 2, job);
            statement.setInt(4, retired);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Durable layout cannot complete its job before execution");
            }
        }
        return proposed;
    }

    private static boolean hasFinalizedInputs(Connection connection, GenerationJobKey job, int stage, int count)
            throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT count(*) FROM artifact_stage_exchanges e
                JOIN artifact_exchange_finalizations f USING (tenant_id,job_id,exchange_id)
                WHERE e.tenant_id=? AND e.job_id=? AND e.consumer_stage_id=?
                """)) {
            bindJob(statement, 1, job);
            statement.setInt(3, stage);
            try (var row = statement.executeQuery()) {
                return row.next() && row.getLong(1) == count;
            }
        }
    }

    private static List<InputRange> readFrozen(Connection connection, GenerationJobKey job, int stage, int count)
            throws SQLException {
        var ranges = new ArrayList<InputRange>(count);
        try (var statement = connection.prepareStatement("""
                SELECT partition_id,exchange_partition_start,exchange_partition_end
                FROM artifact_build_pages WHERE tenant_id=? AND job_id=? AND stage_id=? AND partition_id<?
                ORDER BY partition_id
                """)) {
            bindJob(statement, 1, job);
            statement.setInt(3, stage);
            statement.setInt(4, count);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    if (rows.getInt(1) != ranges.size()) {
                        throw new IllegalStateException("Frozen Shuffle range has a missing physical producer");
                    }
                    ranges.add(new InputRange(rows.getInt(2), rows.getInt(3)));
                }
            }
        }
        if (ranges.size() != count) throw new IllegalStateException("Frozen Shuffle layout is incomplete");
        return List.copyOf(ranges);
    }

    private static boolean setPhysicalCount(Connection connection, GenerationJobKey job, String exchange, int count)
            throws SQLException {
        try (var statement = connection.prepareStatement("""
                UPDATE artifact_stage_exchanges e SET physical_producer_count=?
                WHERE e.tenant_id=? AND e.job_id=? AND e.exchange_id=? AND e.physical_producer_count IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM artifact_exchange_registrations r
                        WHERE r.tenant_id=e.tenant_id AND r.job_id=e.job_id AND r.exchange_id=e.exchange_id
                    )
                """)) {
            statement.setInt(1, count);
            bindJob(statement, 2, job);
            statement.setString(4, exchange);
            return statement.executeUpdate() == 1;
        }
    }

    private static void freezePages(Connection connection, GenerationJobKey job, int stage, int original,
            List<InputRange> proposed) throws SQLException {
        try (var statement = connection.prepareStatement("""
                WITH ranges AS (
                    SELECT * FROM unnest(?::integer[], ?::integer[])
                        WITH ORDINALITY AS range_bounds(start_partition,end_partition,ordinal)
                ), assignments AS (
                    SELECT p.page_id, r.start_partition, r.end_partition
                    FROM artifact_build_pages p LEFT JOIN ranges r ON r.ordinal=p.partition_id+1
                    WHERE p.tenant_id=? AND p.job_id=? AND p.stage_id=? AND p.partition_id<?
                )
                UPDATE artifact_build_pages p SET exchange_partition_start=a.start_partition,
                    exchange_partition_end=a.end_partition,
                    state=CASE WHEN a.end_partition IS NULL THEN 'COALESCED' ELSE 'PENDING' END
                FROM assignments a
                WHERE p.tenant_id=? AND p.job_id=? AND p.page_id=a.page_id
                    AND p.page_kind='RELATIONAL_COMPUTE' AND p.state='PENDING' AND p.attempt_id=-1
                """)) {
            statement.setObject(1, proposed.stream().mapToInt(InputRange::startInclusive).toArray());
            statement.setObject(2, proposed.stream().mapToInt(InputRange::endExclusive).toArray());
            bindJob(statement, 3, job);
            statement.setInt(5, stage);
            statement.setInt(6, original);
            bindJob(statement, 7, job);
            if (statement.executeUpdate() != original) {
                throw new IllegalStateException("Durable layout cannot change an issued or missing partition");
            }
        }
    }

    private static void requireCoverage(List<InputRange> ranges, int original) {
        int next = 0;
        for (var range : ranges) {
            if (range.startInclusive() != next) {
                throw new IllegalArgumentException("Durable Shuffle ranges must be adjacent and ordered");
            }
            next = range.endExclusive();
        }
        if (ranges.isEmpty() || ranges.size() >= original || next != original) {
            throw new IllegalArgumentException("Durable Shuffle ranges must coalesce the exact original cohort");
        }
    }

    private static void bindJob(PreparedStatement statement, int offset, GenerationJobKey job) throws SQLException {
        statement.setString(offset, job.tenantId());
        statement.setString(offset + 1, job.jobId());
    }
}
