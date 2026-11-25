package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.job.GenerationSubmissionAuthority;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.job.SnapshotPinReservation;
import org.pangrova.persistence.generation.api.job.SnapshotPinState;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
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
    private static final String CLEANUP_PINS_SQL = "SELECT " + QUALIFIED_COLUMNS
            + " FROM artifact_snapshot_pins p "
            + "LEFT JOIN artifact_build_jobs j ON j.tenant_id=p.tenant_id AND j.job_id=p.job_id "
            + "WHERE p.state IN ('INTENDED','PINNED','RELEASING') AND ("
            + "(p.job_id IS NULL AND p.created_at<=?) OR "
            + "(j.state IN ('FAILED','CANCELLED') AND j.updated_at<=?) OR "
            + "(j.state='PUBLISHED' AND NOT EXISTS (SELECT 1 FROM artifact_snapshot_pin_references r "
            + "WHERE r.tenant_id=p.tenant_id AND r.pin_owner=p.pin_owner) AND ("
            + "NOT EXISTS (SELECT 1 FROM index_descriptors d WHERE d.index_id=p.index_id) OR "
            + "EXISTS (SELECT 1 FROM index_descriptors d WHERE d.index_id=p.index_id "
            + "AND ((d.state='DROPPED' AND d.updated_at<=?) OR "
            + "(d.state<>'DROPPED' AND (d.generation<>p.generation "
            + "OR d.build_snapshot_id<>p.source_snapshot) AND d.updated_at<=?)))))) "
            + "ORDER BY p.created_at,p.tenant_id,p.pin_owner LIMIT ?";
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

    static void bindJob(Connection connection, String tenantId, String jobId, String pinOwner)
            throws SQLException {
        SnapshotPinIntent intent = find(connection, tenantId, pinOwner, true)
                .orElseThrow(() -> new IllegalStateException("Snapshot pin intent is absent"));
        if (intent.state() != SnapshotPinState.PINNED || intent.jobId() != null
                && !intent.jobId().equals(jobId)) {
            throw new IllegalStateException("Durable job requires its exact PINNED snapshot intent");
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET job_id=?, updated_at=? "
                        + "WHERE tenant_id=? AND pin_owner=? AND state='PINNED'")) {
            statement.setString(1, jobId);
            setInstant(statement, 2, Instant.now());
            statement.setString(3, tenantId);
            statement.setString(4, pinOwner);
            statement.executeUpdate();
        }
    }

    static List<SnapshotPinIntent> cleanupCandidates(
            Connection connection, int limit, Instant orphanCutoff, Instant droppedCutoff)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(CLEANUP_PINS_SQL)) {
            setInstant(statement, 1, orphanCutoff);
            setInstant(statement, 2, orphanCutoff);
            setInstant(statement, 3, droppedCutoff);
            setInstant(statement, 4, droppedCutoff);
            statement.setInt(5, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<SnapshotPinIntent> rows = new ArrayList<>();
                while (result.next()) {
                    rows.add(map(result));
                }
                return List.copyOf(rows);
            }
        }
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
