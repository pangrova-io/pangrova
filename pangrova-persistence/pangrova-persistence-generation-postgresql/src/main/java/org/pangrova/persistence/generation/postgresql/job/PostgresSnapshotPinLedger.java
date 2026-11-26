package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationSubmissionAuthority;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.job.SnapshotPinPressure;
import org.pangrova.persistence.generation.api.job.SnapshotPinReservation;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Same-connection PostgreSQL transitions for durable source snapshot intent and references. */
final class PostgresSnapshotPinLedger {
    private static final String COLUMNS = "tenant_id,pin_owner,index_id,generation,attempt_id,source_id,"
            + "source_snapshot,principal,authorization_scope_bytes,state,job_id,created_at,updated_at,"
            + "pinned_at,released_at";
    private static final String QUALIFIED_COLUMNS = "p.tenant_id,p.pin_owner,p.index_id,p.generation,p.attempt_id,"
            + "p.source_id,p.source_snapshot,p.principal,p.authorization_scope_bytes,p.state,p.job_id,"
            + "p.created_at,p.updated_at,p.pinned_at,p.released_at";
    private static final String RESERVE_PIN_SQL = "INSERT INTO artifact_snapshot_pins (tenant_id,pin_owner,"
            + "index_id,generation,attempt_id,source_id,source_snapshot,principal,authorization_scope_bytes,"
            + "state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,'INTENDED',?,?) ON CONFLICT DO NOTHING";
    private static final String FIND_PIN_SQL = "SELECT " + COLUMNS
            + " FROM artifact_snapshot_pins WHERE tenant_id=? AND pin_owner=?";
    private static final String FIND_PIN_FOR_UPDATE_SQL = FIND_PIN_SQL + " FOR UPDATE";
    private static final String FIND_PIN_FOR_JOB_SQL = "SELECT " + COLUMNS
            + " FROM artifact_snapshot_pins WHERE tenant_id=? AND job_id=?";
    private static final Duration CLEANUP_RETRY_DELAY = Duration.ofMinutes(15);
    private static final String CLEANUP_ELIGIBILITY_SQL = """
             FROM artifact_snapshot_pins p
            LEFT JOIN artifact_build_jobs j ON j.tenant_id=p.tenant_id AND j.job_id=p.job_id
            WHERE p.state IN ('INTENDED','PINNED','RELEASING')
              AND (p.cleanup_retry_at IS NULL OR p.cleanup_retry_at<=?)
              AND NOT EXISTS (SELECT 1 FROM artifact_snapshot_pin_references r
                  WHERE r.tenant_id=p.tenant_id AND r.pin_owner=p.pin_owner)
              AND (
                p.state='RELEASING'
                OR (p.job_id IS NULL AND p.created_at<=? AND NOT EXISTS (
                    SELECT 1 FROM index_descriptors d
                    WHERE d.index_id=p.index_id AND d.tenant_id=p.tenant_id AND d.state<>'DROPPED'
                      AND ((d.state='BUILDING' AND d.generation=p.generation
                        AND d.build_snapshot_id=p.source_snapshot AND d.native_submission_attempt_id=p.attempt_id)
                        OR EXISTS (SELECT 1 FROM index_generation_builds b WHERE b.index_id=d.index_id
                          AND b.generation=p.generation AND b.state='BUILDING' AND b.source_snapshot=p.source_snapshot
                          AND b.native_submission_attempt_id=p.attempt_id))))
                OR (j.state IN ('FAILED','CANCELLED') AND j.updated_at<=?)
                OR (j.state='PUBLISHED' AND (
                    EXISTS (SELECT 1 FROM index_generation_manifests retired
                        JOIN index_descriptors d ON d.index_id=retired.index_id AND d.tenant_id=p.tenant_id
                        WHERE retired.index_id=p.index_id AND retired.generation=p.generation
                          AND retired.source_id=p.source_id AND retired.source_snapshot=p.source_snapshot
                          AND retired.artifact_type=j.artifact_type AND retired.state='RETIRED'
                          AND retired.retired_at<=?)
                    OR (NOT EXISTS (SELECT 1 FROM index_generation_manifests retired
                            WHERE retired.index_id=p.index_id AND retired.generation=p.generation
                              AND retired.state='RETIRED')
                        AND (NOT EXISTS (SELECT 1 FROM index_descriptors d
                                WHERE d.index_id=p.index_id AND d.tenant_id=p.tenant_id)
                            OR EXISTS (SELECT 1 FROM index_descriptors d
                                WHERE d.index_id=p.index_id AND d.tenant_id=p.tenant_id
                                  AND ((d.state='DROPPED' AND d.updated_at<=?)
                                    OR (d.state<>'DROPPED' AND (d.generation<>p.generation
                                          OR d.build_snapshot_id<>p.source_snapshot) AND d.updated_at<=?
                                        AND NOT EXISTS (SELECT 1 FROM index_generation_manifests m
                                            WHERE m.index_id=d.index_id AND m.generation=p.generation
                                              AND m.state='ACTIVE')))))))
                ))
            """;
    private static final String CLEANUP_PINS_SQL = "SELECT " + QUALIFIED_COLUMNS
            + CLEANUP_ELIGIBILITY_SQL + " ORDER BY COALESCE(p.cleanup_retry_at,p.created_at),"
            + "p.created_at,p.tenant_id,p.pin_owner LIMIT ?";
    private static final String CLAIM_CLEANUP_SQL = "SELECT 1" + CLEANUP_ELIGIBILITY_SQL
            + " AND p.tenant_id=? AND p.pin_owner=?";
    // One statement and at most cap+1 index entries per state. No inventory or
    // retained-file scan, and no connector calls from this operational sample.
    private static final String PRESSURE_SQL = """
            SELECT states.state, observed.pin_count, observed.oldest
            FROM (VALUES ('INTENDED'),('PINNED'),('RELEASING')) AS states(state)
            CROSS JOIN LATERAL (
                SELECT COUNT(*) AS pin_count, MIN(created_at) AS oldest
                FROM (SELECT created_at FROM artifact_snapshot_pins
                      WHERE state=states.state ORDER BY created_at LIMIT ?) AS bounded
            ) AS observed
            ORDER BY states.state
            """;

    private static final String UNBOUND_RECOVERY_ELIGIBILITY_SQL = " FROM artifact_snapshot_pins p "
            + "JOIN index_descriptors d ON d.index_id=p.index_id AND d.tenant_id=p.tenant_id "
            + "WHERE p.job_id IS NULL AND p.state IN ('INTENDED','PINNED') "
            + "AND p.updated_at<=? AND ((d.state='BUILDING' AND d.generation=p.generation "
            + "AND d.build_snapshot_id=p.source_snapshot AND d.native_submission_attempt_id=p.attempt_id) OR "
            + "(d.state IN ('READY','STALE') AND EXISTS (SELECT 1 FROM index_generation_builds b "
            + "WHERE b.index_id=d.index_id AND b.generation=p.generation AND b.base_generation=d.generation "
            + "AND b.state='BUILDING' AND b.source_snapshot=p.source_snapshot "
            + "AND b.native_submission_attempt_id=p.attempt_id)))";
    private static final String UNBOUND_RECOVERY_SQL = "SELECT " + QUALIFIED_COLUMNS
            + UNBOUND_RECOVERY_ELIGIBILITY_SQL + " ORDER BY p.updated_at,p.tenant_id,p.pin_owner LIMIT ?";
    private static final String CLAIM_RECOVERY_SQL = "SELECT 1" + UNBOUND_RECOVERY_ELIGIBILITY_SQL
            + " AND p.tenant_id=? AND p.pin_owner=?";
    private static final String ADOPT_PINS_SQL = "SELECT " + QUALIFIED_COLUMNS
            + " FROM artifact_snapshot_pins p "
            + "JOIN artifact_build_jobs j ON j.tenant_id=p.tenant_id AND j.job_id=p.job_id "
            + "WHERE p.state='INTENDED' AND p.attempt_id LIKE 'legacy-%' AND j.state='PUBLISHED' "
            + "ORDER BY p.created_at,p.tenant_id,p.pin_owner LIMIT ?";
    private static final String RETAIN_PIN_SQL = "SELECT p.pin_owner FROM artifact_snapshot_pins p "
            + "JOIN artifact_build_jobs j ON j.tenant_id=p.tenant_id AND j.job_id=p.job_id "
            + "JOIN index_descriptors d ON d.index_id=p.index_id AND d.state<>'DROPPED' "
            + "JOIN execution_anchor a ON a.execution_id=? "
            + "WHERE p.tenant_id=? AND p.index_id=? AND p.generation=? "
            + "AND p.source_snapshot=? AND p.state='PINNED' AND j.state='PUBLISHED' "
            + "AND j.staging_generation=? FOR UPDATE OF p";

    private PostgresSnapshotPinLedger() {}

    static SnapshotPinIntent reserve(
            Connection connection, GenerationTransactionAuthority authority,
            SnapshotPinReservation request) throws SQLException {
        Objects.requireNonNull(request, "request");
        authority.requireCurrent(connection, request.tenantId(), request.authority().principal(),
                request.authority().authorizationScope());
        try (PreparedStatement statement = connection.prepareStatement(RESERVE_PIN_SQL)) {
            int parameter = 1;
            statement.setString(parameter++, request.tenantId());
            statement.setString(parameter++, owner(request));
            statement.setString(parameter++, request.indexId());
            statement.setInt(parameter++, request.generation());
            statement.setString(parameter++, request.attemptId());
            statement.setString(parameter++, request.sourceId());
            statement.setString(parameter++, request.sourceSnapshot());
            statement.setString(parameter++, request.authority().principal());
            statement.setBytes(parameter++, DeferredAuthorizationScopeCodec.encode(
                    request.authority().authorizationScope()));
            setInstant(statement, parameter++, request.createdAt());
            setInstant(statement, parameter, request.createdAt());
            statement.executeUpdate();
        }
        SnapshotPinIntent existing = find(connection, request.tenantId(), owner(request), true)
                .orElseThrow(() -> new IllegalStateException("Snapshot pin intent was not persisted"));
        if (existing.state() == SnapshotPinState.RELEASED
                || existing.state() == SnapshotPinState.RELEASING
                || !existing.sourceId().equals(request.sourceId())
                || !existing.sourceSnapshot().equals(request.sourceSnapshot())
                || !existing.authority().equals(request.authority())) {
            throw new IllegalStateException("Snapshot pin owner is already bound to different or released work");
        }
        return existing;
    }

    static boolean markPinned(Connection connection, String tenantId, String pinOwner, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET state='PINNED', pinned_at=COALESCE(pinned_at,?), "
                        + "updated_at=? WHERE tenant_id=? AND pin_owner=? AND state IN ('INTENDED','PINNED')")) {
            setInstant(statement, 1, now);
            setInstant(statement, 2, now);
            statement.setString(3, tenantId);
            statement.setString(4, pinOwner);
            statement.executeUpdate();
        }
        return find(connection, tenantId, pinOwner, true)
                .map(intent -> intent.state() == SnapshotPinState.PINNED).orElse(false);
    }

    static boolean beginRelease(Connection connection, String tenantId, String pinOwner, Instant now)
            throws SQLException {
        SnapshotPinState state;
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state FROM artifact_snapshot_pins WHERE tenant_id=? AND pin_owner=? FOR UPDATE")) {
            statement.setString(1, tenantId);
            statement.setString(2, pinOwner);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return false;
                }
                state = SnapshotPinState.valueOf(row.getString("state"));
            }
        }
        if (state == SnapshotPinState.RELEASED || hasReference(connection, tenantId, pinOwner)) {
            return false;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET state='RELEASING', pinned_at=COALESCE(pinned_at,?), "
                        + "updated_at=? WHERE tenant_id=? AND pin_owner=? "
                        + "AND state IN ('INTENDED','PINNED','RELEASING')")) {
            setInstant(statement, 1, now);
            setInstant(statement, 2, now);
            statement.setString(3, tenantId);
            statement.setString(4, pinOwner);
            statement.executeUpdate();
        }
        return true;
    }

    static Optional<SnapshotPinIntent> claimCleanup(
            Connection connection, String tenantId, String pinOwner,
            Instant orphanCutoff, Instant droppedCutoff, Instant now) throws SQLException {
        // Lock the exact pin before reading eligibility in a subsequent READ COMMITTED statement.
        // Job binding and execution retention both acquire this same row lock.
        if (find(connection, tenantId, pinOwner, true).isEmpty()) {
            return Optional.empty();
        }
        try (PreparedStatement statement = connection.prepareStatement(CLAIM_CLEANUP_SQL)) {
            bindCleanupCutoffs(statement, orphanCutoff, droppedCutoff, now);
            statement.setString(7, tenantId);
            statement.setString(8, pinOwner);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
            }
        }
        if (!beginRelease(connection, tenantId, pinOwner, now)) {
            return Optional.empty();
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET cleanup_retry_at=? WHERE tenant_id=? AND pin_owner=?")) {
            setInstant(statement, 1, now.plus(CLEANUP_RETRY_DELAY));
            statement.setString(2, tenantId);
            statement.setString(3, pinOwner);
            statement.executeUpdate();
        }
        return find(connection, tenantId, pinOwner, false);
    }

    static boolean markReleased(Connection connection, String tenantId, String pinOwner, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET state='RELEASED', "
                        + "released_at=COALESCE(released_at,?), updated_at=? "
                        + "WHERE tenant_id=? AND pin_owner=? AND state='RELEASING'")) {
            setInstant(statement, 1, now);
            setInstant(statement, 2, now);
            statement.setString(3, tenantId);
            statement.setString(4, pinOwner);
            statement.executeUpdate();
        }
        return find(connection, tenantId, pinOwner, true)
                .map(intent -> intent.state() == SnapshotPinState.RELEASED).orElse(false);
    }

    static Optional<SnapshotPinIntent> find(
            Connection connection, String tenantId, String pinOwner, boolean lock) throws SQLException {
        PreparedStatement statement = lock
                ? connection.prepareStatement(FIND_PIN_FOR_UPDATE_SQL)
                : connection.prepareStatement(FIND_PIN_SQL);
        try (statement) {
            statement.setString(1, tenantId);
            statement.setString(2, pinOwner);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(map(result)) : Optional.empty();
            }
        }
    }

    static Optional<SnapshotPinIntent> findForJob(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(FIND_PIN_FOR_JOB_SQL)) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(map(result)) : Optional.empty();
            }
        }
    }

    static void bindJob(Connection connection, GenerationJob job, GenerationDispatchSpec dispatchSpec,
                        String pinOwner) throws SQLException {
        SnapshotPinIntent intent = find(connection, job.tenantId(), pinOwner, true)
                .orElseThrow(() -> new IllegalStateException("Snapshot pin intent is absent"));
        int generation = Integer.parseInt(job.stagingGeneration());
        if (intent.state() != SnapshotPinState.PINNED
                || intent.jobId() != null && !intent.jobId().equals(job.jobId())
                || !intent.indexId().equals(job.artifactId())
                || intent.generation() != generation
                || !intent.sourceId().equals(job.sourceId())
                || !intent.sourceSnapshot().equals(job.sourceSnapshot())
                || !intent.authority().tenantId().equals(dispatchSpec.tenantId())
                || !intent.authority().principal().equals(dispatchSpec.principal())
                || !intent.authority().authorizationScope().equals(dispatchSpec.authorizationScope())) {
            throw new IllegalStateException("Durable job requires its exact PINNED snapshot intent");
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET job_id=?, updated_at=? "
                        + "WHERE tenant_id=? AND pin_owner=? AND state='PINNED'")) {
            statement.setString(1, job.jobId());
            setInstant(statement, 2, Instant.now());
            statement.setString(3, job.tenantId());
            statement.setString(4, pinOwner);
            statement.executeUpdate();
        }
    }

    static SnapshotPinPressure observePressure(
            Connection connection, int maxPerState, Instant observedAt) throws SQLException {
        if (maxPerState <= 0 || maxPerState > SnapshotPinPressure.MAX_PER_STATE) {
            throw new IllegalArgumentException("Native pin pressure observation limit must be between 1 and 1000");
        }
        Objects.requireNonNull(observedAt, "observedAt");
        try (PreparedStatement statement = connection.prepareStatement(PRESSURE_SQL)) {
            statement.setQueryTimeout(5);
            statement.setInt(1, Math.addExact(maxPerState, 1));
            try (ResultSet result = statement.executeQuery()) {
                List<SnapshotPinPressure.StateCount> states = new ArrayList<>();
                while (result.next()) {
                    int count = result.getInt("pin_count");
                    states.add(new SnapshotPinPressure.StateCount(
                            SnapshotPinState.valueOf(result.getString("state")),
                            Math.min(count, maxPerState), count > maxPerState,
                            Optional.ofNullable(getInstant(result, "oldest"))));
                }
                return new SnapshotPinPressure(observedAt, states);
            }
        }
    }

    static List<SnapshotPinIntent> cleanupCandidates(
            Connection connection, int limit, Instant orphanCutoff, Instant droppedCutoff)
            throws SQLException {
        return cleanupCandidates(connection, limit, orphanCutoff, droppedCutoff, Instant.now());
    }

    static List<SnapshotPinIntent> cleanupCandidates(
            Connection connection, int limit, Instant orphanCutoff, Instant droppedCutoff, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(CLEANUP_PINS_SQL)) {
            bindCleanupCutoffs(statement, orphanCutoff, droppedCutoff, now);
            statement.setInt(7, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<SnapshotPinIntent> rows = new ArrayList<>();
                while (result.next()) {
                    rows.add(map(result));
                }
                return List.copyOf(rows);
            }
        }
    }

    private static void bindCleanupCutoffs(
            PreparedStatement statement, Instant orphanCutoff, Instant droppedCutoff, Instant now)
            throws SQLException {
        setInstant(statement, 1, now);
        setInstant(statement, 2, orphanCutoff);
        setInstant(statement, 3, orphanCutoff);
        setInstant(statement, 4, droppedCutoff);
        setInstant(statement, 5, droppedCutoff);
        setInstant(statement, 6, droppedCutoff);
    }

    static List<SnapshotPinIntent> listForRecovery(
            Connection connection, int limit, Instant cutoff) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UNBOUND_RECOVERY_SQL)) {
            setInstant(statement, 1, cutoff);
            statement.setInt(2, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<SnapshotPinIntent> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(map(rows));
                }
                return List.copyOf(result);
            }
        }
    }

    static Optional<SnapshotPinIntent> claimRecovery(
            Connection connection, String tenantId, String pinOwner, Instant cutoff, Instant now) throws SQLException {
        if (find(connection, tenantId, pinOwner, true).isEmpty()) {
            return Optional.empty();
        }
        try (PreparedStatement statement = connection.prepareStatement(CLAIM_RECOVERY_SQL)) {
            setInstant(statement, 1, cutoff);
            statement.setString(2, tenantId);
            statement.setString(3, pinOwner);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
            }
        }
        // Persist scheduling progress before connector IO so a persistently unavailable
        // source does not monopolize the oldest bounded batch.
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET updated_at=? WHERE tenant_id=? AND pin_owner=?")) {
            setInstant(statement, 1, now);
            statement.setString(2, tenantId);
            statement.setString(3, pinOwner);
            statement.executeUpdate();
        }
        return find(connection, tenantId, pinOwner, false);
    }

    static List<SnapshotPinIntent> listForAdoption(Connection connection, int limit) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(ADOPT_PINS_SQL)) {
            statement.setInt(1, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<SnapshotPinIntent> rows = new ArrayList<>();
                while (result.next()) {
                    rows.add(map(result));
                }
                return List.copyOf(rows);
            }
        }
    }

    static void retainForExecution(
            Connection connection, String tenantId, String artifactId, String generation,
            String sourceSnapshot, String executionId) throws SQLException {
        int generationValue = Integer.parseInt(generation);
        String pinOwner;
        try (PreparedStatement statement = connection.prepareStatement(RETAIN_PIN_SQL)) {
            statement.setString(1, executionId);
            statement.setString(2, tenantId);
            statement.setString(3, artifactId);
            statement.setInt(4, generationValue);
            statement.setString(5, sourceSnapshot);
            statement.setString(6, generation);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("Published artifact has no retainable live snapshot pin");
                }
                pinOwner = row.getString("pin_owner");
                if (row.next()) {
                    throw new IllegalStateException("Published artifact resolves to multiple snapshot pins");
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO artifact_snapshot_pin_references (tenant_id,pin_owner,execution_id) "
                        + "VALUES (?,?,?) ON CONFLICT DO NOTHING")) {
            statement.setString(1, tenantId);
            statement.setString(2, pinOwner);
            statement.setString(3, executionId);
            statement.executeUpdate();
        }
    }

    private static boolean hasReference(
            Connection connection, String tenantId, String pinOwner) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM artifact_snapshot_pin_references WHERE tenant_id=? AND pin_owner=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, pinOwner);
            try (ResultSet row = statement.executeQuery()) {
                return row.next();
            }
        }
    }

    private static SnapshotPinIntent map(ResultSet result) throws SQLException {
        return new SnapshotPinIntent(
                result.getString("tenant_id"), result.getString("index_id"),
                result.getInt("generation"), result.getString("attempt_id"),
                result.getString("source_id"), result.getString("source_snapshot"),
                new GenerationSubmissionAuthority(result.getString("tenant_id"), result.getString("principal"),
                        DeferredAuthorizationScopeCodec.decode(result.getBytes("authorization_scope_bytes"))),
                SnapshotPinState.valueOf(result.getString("state")), result.getString("job_id"),
                getInstant(result, "created_at"), getInstant(result, "updated_at"));
    }

    private static String owner(SnapshotPinReservation request) {
        return request.indexId() + ":generation:" + request.generation() + ":attempt:" + request.attemptId();
    }
}
