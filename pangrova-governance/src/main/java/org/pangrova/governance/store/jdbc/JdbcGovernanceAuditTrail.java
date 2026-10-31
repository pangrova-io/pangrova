package org.pangrova.governance.store.jdbc;

import org.pangrova.governance.api.audit.GovernanceAuditEntry;
import org.pangrova.governance.api.audit.GovernanceAuditTrail;
import org.pangrova.governance.api.audit.OperationType;
import org.pangrova.governance.core.audit.InMemoryGovernanceAuditTrail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * JDBC-backed governance audit trail persisting entries to the
 * {@code governance_audit_trail} table.
 *
 * <p>Replaces {@link InMemoryGovernanceAuditTrail} for production deployments,
 * ensuring audit records survive process restarts.</p>
 */
public final class JdbcGovernanceAuditTrail implements GovernanceAuditTrail {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGovernanceAuditTrail.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private static final String SQL_INSERT =
            "INSERT INTO governance_audit_trail "
                    + "(timestamp_utc, actor, operation_type, resource_path, before_state, after_state, details) "
                    + "VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?) "
                    + "RETURNING id, timestamp_utc";

    private static final String SQL_BY_RESOURCE =
            "SELECT * FROM governance_audit_trail WHERE resource_path = ? ORDER BY timestamp_utc DESC";

    private static final String SQL_BY_ACTOR =
            "SELECT * FROM governance_audit_trail WHERE actor = ? ORDER BY timestamp_utc DESC";

    private static final String SQL_BY_RANGE =
            "SELECT * FROM governance_audit_trail "
                    + "WHERE timestamp_utc >= ? AND timestamp_utc < ? ORDER BY timestamp_utc DESC";

    private static final String SQL_COUNT =
            "SELECT COUNT(*) FROM governance_audit_trail";

    private final DataSource dataSource;

    public JdbcGovernanceAuditTrail(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public GovernanceAuditEntry record(
            String actor,
            OperationType operationType,
            String resourcePath,
            Map<String, String> beforeState,
            Map<String, String> afterState,
            String details) {
        Objects.requireNonNull(actor, "actor must not be null");
        Objects.requireNonNull(operationType, "operationType must not be null");
        Objects.requireNonNull(resourcePath, "resourcePath must not be null");

        Instant now = Instant.now();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setTimestamp(1, Timestamp.from(now));
            ps.setString(2, actor);
            ps.setString(3, operationType.name());
            ps.setString(4, resourcePath);
            ps.setString(5, serializeMap(beforeState));
            ps.setString(6, serializeMap(afterState));
            ps.setString(7, details != null ? details : "");

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long id = rs.getLong("id");
                    Instant ts = rs.getTimestamp("timestamp_utc").toInstant();
                    GovernanceAuditEntry entry = new GovernanceAuditEntry(
                            id, ts, actor, operationType, resourcePath,
                            beforeState, afterState, details);
                    LOG.info("event=governance_audit_recorded id={} actor={} type={} resource={}",
                            id, actor, operationType, resourcePath);
                    return entry;
                }
            }
            throw new RuntimeException("INSERT RETURNING did not produce a result row");
        } catch (SQLException e) {
            LOG.error("event=governance_audit_record_error actor={} type={} resource={} error={}",
                    actor, operationType, resourcePath, e.getMessage(), e);
            throw new RuntimeException("Failed to record governance audit entry", e);
        }
    }

    @Override
    public List<GovernanceAuditEntry> getEntriesForResource(String resourcePath) {
        Objects.requireNonNull(resourcePath, "resourcePath must not be null");
        return queryEntries(SQL_BY_RESOURCE, resourcePath);
    }

    @Override
    public List<GovernanceAuditEntry> getEntriesByActor(String actor) {
        Objects.requireNonNull(actor, "actor must not be null");
        return queryEntries(SQL_BY_ACTOR, actor);
    }

    @Override
    public List<GovernanceAuditEntry> getEntriesInRange(Instant from, Instant to) {
        Objects.requireNonNull(from, "from must not be null");
        Objects.requireNonNull(to, "to must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_BY_RANGE)) {
            ps.setTimestamp(1, Timestamp.from(from));
            ps.setTimestamp(2, Timestamp.from(to));
            try (ResultSet rs = ps.executeQuery()) {
                return mapRows(rs);
            }
        } catch (SQLException e) {
            LOG.error("event=governance_audit_query_error query=byRange error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to query governance audit entries by range", e);
        }
    }

    @Override
    public long entryCount() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_COUNT);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            LOG.error("event=governance_audit_count_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to count governance audit entries", e);
        }
    }

    private List<GovernanceAuditEntry> queryEntries(String sql, String param) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                return mapRows(rs);
            }
        } catch (SQLException e) {
            LOG.error("event=governance_audit_query_error sql={} param={} error={}",
                    sql, param, e.getMessage(), e);
            throw new RuntimeException("Failed to query governance audit entries", e);
        }
    }

    private static List<GovernanceAuditEntry> mapRows(ResultSet rs) throws SQLException {
        List<GovernanceAuditEntry> result = new ArrayList<>();
        while (rs.next()) {
            result.add(mapRow(rs));
        }
        return Collections.unmodifiableList(result);
    }

    private static GovernanceAuditEntry mapRow(ResultSet rs) throws SQLException {
        return new GovernanceAuditEntry(
                rs.getLong("id"),
                rs.getTimestamp("timestamp_utc").toInstant(),
                rs.getString("actor"),
                OperationType.valueOf(rs.getString("operation_type")),
                rs.getString("resource_path"),
                deserializeMap(rs.getString("before_state")),
                deserializeMap(rs.getString("after_state")),
                rs.getString("details"));
    }

    private static String serializeMap(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize map to JSON", e);
        }
    }

    private static Map<String, String> deserializeMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON to map", e);
        }
    }
}
