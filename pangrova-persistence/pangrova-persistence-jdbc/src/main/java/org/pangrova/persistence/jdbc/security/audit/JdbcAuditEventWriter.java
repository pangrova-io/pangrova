package org.pangrova.persistence.jdbc.security.audit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.pangrova.spi.security.audit.AccessedObject;
import org.pangrova.spi.security.audit.AuditEvent;
import org.pangrova.spi.security.audit.AuditEventQuery;
import org.pangrova.spi.security.audit.AuditEventType;
import org.pangrova.spi.security.audit.AuditEventWriter;
import org.pangrova.persistence.api.security.audit.QueryableAuditEventWriter;

/**
 * Durable, queryable {@link AuditEventWriter} that persists audit events to a
 * relational {@code audit_logs} table via JDBC.
 *
 * <p>Unlike a log-only writer, events written here survive process
 * restart and can be read back with {@link #query(AuditEventQuery)} filtered by
 * principal, type, and time window — the durability + queryability contract
 * required for SOX/GDPR audit-trail and incident response (F-CHAIN-SEC-05 /
 * G-SEC-05).</p>
 *
 * <p>The DDL uses ANSI-compatible types ({@code VARCHAR}, {@code TIMESTAMP},
 * {@code BOOLEAN}, {@code BIGINT}, {@code TEXT}) and {@code CREATE TABLE IF NOT
 * EXISTS}; it is validated against PostgreSQL. The backend is selected at
 * construction by injecting the appropriate {@link DataSource}.</p>
 */
public final class JdbcAuditEventWriter implements QueryableAuditEventWriter {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcAuditEventWriter.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<AccessedObject>> ACCESSED_OBJECTS_TYPE =
            new TypeReference<>() {};
    private static final TypeReference<Map<String, String>> PROPERTIES_TYPE =
            new TypeReference<>() {};

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS audit_logs (
                event_id        VARCHAR(64) PRIMARY KEY,
                event_type      VARCHAR(64) NOT NULL,
                event_time      TIMESTAMP NOT NULL,
                principal       VARCHAR(256),
                client_ip       VARCHAR(64),
                trace_id        VARCHAR(128),
                span_id         VARCHAR(128),
                query_id        VARCHAR(128),
                sql_text        TEXT,
                catalog_name    VARCHAR(256),
                schema_name     VARCHAR(256),
                resource        VARCHAR(1024),
                success         BOOLEAN,
                error_message   TEXT,
                duration_ms     BIGINT,
                rows_read       BIGINT,
                bytes_read      BIGINT,
                accessed_objects TEXT,
                properties      TEXT
            )
            """;

    private static final String INSERT = """
            INSERT INTO audit_logs (
                event_id, event_type, event_time, principal, client_ip,
                trace_id, span_id, query_id, sql_text, catalog_name, schema_name,
                resource, success, error_message, duration_ms, rows_read,
                bytes_read, accessed_objects, properties
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """;

    private final DataSource dataSource;

    public JdbcAuditEventWriter(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute(CREATE_TABLE);
            LOG.info("event=audit_store_initialized backend=jdbc table=audit_logs");
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to initialize audit_logs table: " + e.getMessage(), e);
        }
    }

    @Override
    public void write(AuditEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT)) {
            int parameterIndex = 1;
            ps.setString(parameterIndex++, event.eventId());
            ps.setString(parameterIndex++, event.eventType() != null ? event.eventType().name() : null);
            ps.setTimestamp(parameterIndex++, Timestamp.from(
                    event.timestamp() != null ? event.timestamp() : Instant.now()));
            ps.setString(parameterIndex++, event.principal());
            ps.setString(parameterIndex++, event.clientIp());
            ps.setString(parameterIndex++, event.traceId());
            ps.setString(parameterIndex++, event.spanId());
            ps.setString(parameterIndex++, event.queryId());
            ps.setString(parameterIndex++, event.sql());
            ps.setString(parameterIndex++, event.catalog());
            ps.setString(parameterIndex++, event.schema());
            ps.setString(parameterIndex++, resourceOf(event));
            ps.setBoolean(parameterIndex++, event.success());
            ps.setString(parameterIndex++, event.errorMessage());
            ps.setLong(parameterIndex++, event.durationMs());
            ps.setLong(parameterIndex++, event.rowsRead());
            ps.setLong(parameterIndex++, event.bytesRead());
            ps.setString(parameterIndex++, toJson(event.accessedObjects()));
            ps.setString(parameterIndex, toJson(event.properties()));
            ps.executeUpdate();
        } catch (SQLException e) {
            // Surfaced to the emitter, which isolates the failure (never propagates
            // to the request path) and counts it.
            throw new AuditPersistenceException(
                    "Failed to persist audit event " + event.eventId(), e);
        }
    }

    @Override
    public List<AuditEvent> query(AuditEventQuery query) {
        Objects.requireNonNull(query, "query must not be null");
        StringBuilder sql = new StringBuilder("SELECT * FROM audit_logs WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (query.principal() != null) {
            sql.append(" AND principal = ?");
            params.add(query.principal());
        }
        if (query.eventType() != null) {
            sql.append(" AND event_type = ?");
            params.add(query.eventType().name());
        }
        if (query.from() != null) {
            sql.append(" AND event_time >= ?");
            params.add(Timestamp.from(query.from()));
        }
        if (query.to() != null) {
            sql.append(" AND event_time <= ?");
            params.add(Timestamp.from(query.to()));
        }
        sql.append(" ORDER BY event_time DESC");

        List<AuditEvent> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.setMaxRows(query.limit());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new AuditPersistenceException("Failed to query audit events", e);
        }
        return results;
    }

    private static String resourceOf(AuditEvent event) {
        if (event.accessedObjects() == null || event.accessedObjects().isEmpty()) {
            return null;
        }
        AccessedObject obj = event.accessedObjects().get(0);
        return obj.catalog() + "." + obj.schema() + "." + obj.table();
    }

    private static AuditEvent mapRow(ResultSet rs) throws SQLException {
        Timestamp ts = rs.getTimestamp("event_time");
        return new AuditEvent(
                rs.getString("event_id"),
                AuditEventType.valueOf(rs.getString("event_type")),
                ts != null ? ts.toInstant() : null,
                rs.getString("principal"),
                rs.getString("client_ip"),
                rs.getString("trace_id"),
                rs.getString("span_id"),
                rs.getString("query_id"),
                rs.getString("sql_text"),
                rs.getString("catalog_name"),
                rs.getString("schema_name"),
                fromJson(rs.getString("accessed_objects"), ACCESSED_OBJECTS_TYPE, List.of()),
                rs.getBoolean("success"),
                rs.getString("error_message"),
                rs.getLong("duration_ms"),
                rs.getLong("rows_read"),
                rs.getLong("bytes_read"),
                fromJson(rs.getString("properties"), PROPERTIES_TYPE, Map.of()));
    }

    private static String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            LOG.warn("event=audit_json_serialize_failed error={}", e.getMessage());
            return null;
        }
    }

    private static <T> T fromJson(String json, TypeReference<T> type, T fallback) {
        if (json == null || json.isBlank()) {
            return fallback;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            LOG.warn("event=audit_json_deserialize_failed error={}", e.getMessage());
            return fallback;
        }
    }

    /** Unchecked wrapper so JDBC failures flow through the emitter's fault isolation. */
    public static final class AuditPersistenceException extends RuntimeException {
        public AuditPersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
