package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.enums.GenerationExchangeDistribution;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization.BatchRange;
import org.pangrova.generation.api.graph.GenerationStageGraph.Exchange;
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.InputRange;
import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

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
        requireEligibleStage(stage, inputs, outputs);
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
                    requireCoverage(ranges, stage);
                    return ranges;
                }
                if (row.getBoolean("registered") || proposed.isEmpty()) return List.of();
            }
        }
        requireCoverage(proposed, stage);
        var current = findJob(connection, job.tenantId(), job.jobId(), true).orElseThrow();
        if (terminal(current.state())) {
            throw new IllegalStateException("Durable layout requires finalized inputs and a runnable job");
        }
        if (proposed.stream().anyMatch(InputRange::split)) {
            requireCertificates(proposed, stage, inputs, finalizedInputs(connection, job, stageId));
        } else if (!hasFinalizedInputs(connection, job, stageId, inputs.size())) {
            throw new IllegalStateException("Durable layout requires finalized inputs and a runnable job");
        }
        if (!setPhysicalCount(connection, job, exchange, proposed.size())) return List.of();
        freezePages(connection, job, stageId, stage.parallelism(), proposed);
        try (var statement = connection.prepareStatement("""
                UPDATE artifact_build_jobs
                SET completed_pages=completed_pages+?, expected_pages=expected_pages+?
                WHERE tenant_id=? AND job_id=? AND completed_pages+? < expected_pages+?
                """)) {
            int retired = Math.max(0, stage.parallelism() - proposed.size());
            int added = Math.max(0, proposed.size() - stage.parallelism());
            statement.setInt(1, retired);
            statement.setInt(2, added);
            bindJob(statement, 3, job);
            statement.setInt(5, retired);
            statement.setInt(6, added);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Durable layout cannot complete its job before execution");
            }
        }
        return proposed;
    }

    private static void requireEligibleStage(StagePlan stage, List<Exchange> inputs, List<Exchange> outputs) {
        if (!stage.coalescibleHashInputs() || stage.pageKind() != GenerationPageKind.RELATIONAL_COMPUTE
                || inputs.isEmpty() || inputs.size() > 2 || outputs.size() != 1
                || outputs.get(0).distribution() == GenerationExchangeDistribution.BROADCAST
                || inputs.stream().anyMatch(edge -> edge.distribution() != GenerationExchangeDistribution.HASH
                        || edge.partitionCount() != stage.parallelism())) {
            throw new IllegalArgumentException("Stage cannot coalesce the requested durable Shuffle inputs");
        }
    }

    private static Map<String, GenerationExchangeFinalization> finalizedInputs(
            Connection connection, GenerationJobKey job, int stage) throws SQLException {
        var finalized = new HashMap<String, GenerationExchangeFinalization>();
        try (var statement = connection.prepareStatement("""
                SELECT e.exchange_id, f.finalization_bytes, f.finalization_sha256
                FROM artifact_stage_exchanges e
                JOIN artifact_exchange_finalizations f USING (tenant_id,job_id,exchange_id)
                WHERE e.tenant_id=? AND e.job_id=? AND e.consumer_stage_id=?
                """)) {
            bindJob(statement, 1, job);
            statement.setInt(3, stage);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    String edge = rows.getString(1);
                    var receipt = GenerationExchangeEvidenceCodec.decodeFinalization(
                            rows.getBytes(2), rows.getString(3));
                    if (!receipt.exchangeId().equals(edge) || finalized.putIfAbsent(edge, receipt) != null) {
                        throw new IllegalStateException("Durable layout found a different finalized input");
                    }
                }
            }
        }
        return finalized;
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
                SELECT partition_id,exchange_partition_start,exchange_partition_end,exchange_batch_ranges
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
                    ranges.add(PostgresGenerationBatchRanges.decode(rows.getInt(2), rows.getInt(3), rows.getBytes(4)));
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
                WITH ranges AS MATERIALIZED (
                    SELECT start_partition,end_partition,NULLIF(certificates, ''::bytea) AS certificates,ordinal
                    FROM unnest(?::integer[], ?::integer[], ?::bytea[])
                        WITH ORDINALITY AS range_bounds(start_partition,end_partition,certificates,ordinal)
                ), assignments AS (
                    SELECT p.page_id, r.start_partition, r.end_partition, r.certificates
                    FROM artifact_build_pages p LEFT JOIN ranges r ON r.ordinal=p.partition_id+1
                    WHERE p.tenant_id=? AND p.job_id=? AND p.stage_id=? AND p.partition_id<?
                ), updated AS (
                    UPDATE artifact_build_pages p SET exchange_partition_start=a.start_partition,
                        exchange_partition_end=a.end_partition, exchange_batch_ranges=a.certificates,
                        state=CASE WHEN a.end_partition IS NULL THEN 'COALESCED' ELSE 'PENDING' END
                    FROM assignments a
                    WHERE p.tenant_id=? AND p.job_id=? AND p.page_id=a.page_id
                        AND p.page_kind='RELATIONAL_COMPUTE' AND p.state='PENDING' AND p.attempt_id=-1
                    RETURNING p.tenant_id,p.job_id,p.stage_id,p.partition_id,p.next_available_at,p.max_attempts,
                              p.created_at,p.updated_at
                ), added AS (
                    INSERT INTO artifact_build_pages
                        (tenant_id,job_id,page_id,stage_id,partition_id,page_ordinal,page_kind,state,
                         next_available_at,max_attempts,created_at,updated_at,exchange_partition_start,
                         exchange_partition_end,exchange_batch_ranges,adaptive_created)
                    SELECT u.tenant_id,u.job_id,'adaptive:' || gen_random_uuid()::text,u.stage_id,
                           r.ordinal-1,
                           (SELECT max(p.page_ordinal) FROM artifact_build_pages p
                            WHERE p.tenant_id=? AND p.job_id=?) + r.ordinal-?,
                           'RELATIONAL_COMPUTE','PENDING',u.next_available_at,u.max_attempts,
                           u.created_at,u.updated_at,r.start_partition,r.end_partition,r.certificates,TRUE
                    FROM ranges r JOIN updated u ON u.partition_id=r.start_partition
                    WHERE r.ordinal>?
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM updated), (SELECT count(*) FROM added)
                """)) {
            statement.setObject(1, proposed.stream().mapToInt(InputRange::startInclusive).toArray());
            statement.setObject(2, proposed.stream().mapToInt(InputRange::endExclusive).toArray());
            statement.setObject(3, proposed.stream().map(PostgresGenerationBatchRanges::encode).toArray(byte[][]::new));
            bindJob(statement, 4, job);
            statement.setInt(6, stage);
            statement.setInt(7, original);
            bindJob(statement, 8, job);
            int lastJobParameter = 10;
            bindJob(statement, lastJobParameter, job);
            statement.setInt(lastJobParameter + 2, original);
            statement.setInt(lastJobParameter + 3, original);
            try (var result = statement.executeQuery()) {
                if (!result.next() || result.getLong(1) != original
                        || result.getLong(2) != Math.max(0, proposed.size() - original)) {
                    throw new IllegalStateException("Durable layout cannot change an issued or missing partition");
                }
            }
        }
    }

    private static void requireCoverage(List<InputRange> ranges, StagePlan stage) {
        int next = 0;
        InputRange previous = null;
        boolean split = false;
        for (var range : ranges) {
            if (range.startInclusive() == next) {
                next = range.endExclusive();
            } else if (previous == null || !previous.split() || !range.split()
                    || range.startInclusive() != previous.startInclusive() || range.endExclusive() != next) {
                throw new IllegalArgumentException("Durable Shuffle ranges must be adjacent and ordered");
            }
            split |= range.split();
            previous = range;
        }
        int original = stage.parallelism();
        if (ranges.isEmpty() || next != original || (!split && ranges.size() >= original)
                || (split && (stage.splittableHashInputs().isEmpty()
                        || ranges.size() > GenerationExchangeFinalization.maximumAdaptiveTasks(original)))) {
            throw new IllegalArgumentException("Durable Shuffle ranges must cover the exact original cohort");
        }
    }

    private static void requireCertificates(List<InputRange> ranges, StagePlan stage, List<Exchange> inputs,
            Map<String, GenerationExchangeFinalization> finalized) {
        if (inputs.size() != 2 || finalized.size() != inputs.size() || inputs.stream().anyMatch(edge ->
                !finalized.containsKey(edge.exchangeId())
                        || finalized.get(edge.exchangeId()).partitions().size() != edge.partitionCount())) {
            throw new IllegalArgumentException("Durable skew layout requires both finalized inputs");
        }
        for (int first = 0; first < ranges.size();) {
            var range = ranges.get(first);
            int last = first + 1;
            while (last < ranges.size() && ranges.get(last).startInclusive() == range.startInclusive()) last++;
            if (range.split()) {
                requireBucket(ranges.subList(first, last), stage, inputs, finalized);
            }
            first = last;
        }
    }

    private static void requireBucket(List<InputRange> bucket, StagePlan stage, List<Exchange> inputs,
            Map<String, GenerationExchangeFinalization> finalized) {
        if (bucket.size() > GenerationExchangeFinalization.MAX_BATCH_RANGES
                || new HashSet<>(bucket).size() != bucket.size()) {
            throw new IllegalArgumentException("Durable skew bucket repeats or exceeds its physical task bounds");
        }
        int firstCount = requireSide(bucket, stage, inputs.get(0), finalized, InputRange::first);
        int secondCount = requireSide(bucket, stage, inputs.get(1), finalized, InputRange::second);
        if (firstCount * secondCount != bucket.size()) {
            throw new IllegalArgumentException("Durable skew bucket does not cover every certified pair");
        }
    }

    private static int requireSide(List<InputRange> bucket, StagePlan stage, Exchange edge,
            Map<String, GenerationExchangeFinalization> finalized, Function<InputRange, BatchRange> select) {
        var ranges = new TreeMap<Long, BatchRange>();
        boolean split = select.apply(bucket.get(0)) != null;
        for (var task : bucket) {
            var range = select.apply(task);
            if ((range != null) != split) {
                throw new IllegalArgumentException("Durable skew mixes whole reads and certificates on one side");
            }
            if (range == null) continue;
            var previous = ranges.putIfAbsent(range.startSequence(), range);
            if (previous != null && !previous.equals(range)) {
                throw new IllegalArgumentException("Durable skew bucket stores inconsistent range certificates");
            }
        }
        if (!split) return 1;
        if (!stage.splittableHashInputs().contains(edge.exchangeId())) {
            throw new IllegalArgumentException("Durable skew changes the preserved side of a join");
        }
        int partition = bucket.get(0).startInclusive();
        finalized.get(edge.exchangeId()).partitions().get(partition).requireRangeIndex(List.copyOf(ranges.values()));
        return ranges.size();
    }

    private static void bindJob(PreparedStatement statement, int offset, GenerationJobKey job) throws SQLException {
        statement.setString(offset, job.tenantId());
        statement.setString(offset + 1, job.jobId());
    }
}
