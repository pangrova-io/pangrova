package org.pangrova.metadata.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.spi.security.tenant.ResourceQuota;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.spi.security.tenant.ResolvedTenantContext;
import org.pangrova.persistence.api.security.tenant.TenantStore;
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
import java.util.Optional;

/**
 * JDBC-backed implementation of {@link TenantStore}.
 *
 * <p>Persists tenant definitions and user-tenant memberships to the
 * {@code tenants} and {@code tenant_user_memberships} tables.
 * Uses PostgreSQL {@code INSERT ... ON CONFLICT} for upsert
 * semantics.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking.
 * No in-process mutable state.</p>
 */
@ThreadSafe
public final class JdbcTenantStore implements TenantStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTenantStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<String>> LIST_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    // ── PreparedStatement parameter indices for SQL_MERGE_TENANT ─────

    private static final int PARAM_TENANT_ID = 1;
    private static final int PARAM_NAME = 2;
    private static final int PARAM_STATUS = 3;
    private static final int PARAM_MAX_MEMORY_BYTES = 4;
    private static final int PARAM_MAX_CONCURRENT_QUERIES = 5;
    private static final int PARAM_MAX_QUEUED_QUERIES = 6;
    private static final int PARAM_MAX_CPU_TIME_PER_QUERY_MS = 7;
    private static final int PARAM_MAX_SCAN_BYTES_PER_QUERY = 8;
    private static final int PARAM_MAX_DAILY_QUERY_COUNT = 9;
    private static final int PARAM_MAX_DAILY_SCAN_BYTES = 10;
    private static final int PARAM_MAX_STORAGE_BYTES = 11;
    private static final int PARAM_ALLOWED_CATALOGS = 12;
    private static final int PARAM_ALLOWED_SCHEMAS = 13;
    private static final int PARAM_RESOURCE_POOL = 14;
    private static final int PARAM_PROPERTIES = 15;
    private static final int PARAM_CREATED_AT = 16;
    private static final int PARAM_UPDATED_AT = 17;
    private static final int PARAM_CREATED_BY = 18;

    /** All-zero quota used when a tenant carries no explicit quota. */
    private static final ResourceQuota EMPTY_QUOTA = new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0);

    // ── SQL Constants ────────────────────────────────────────────────

    static final String DDL_TENANTS = """
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id          VARCHAR(255)  NOT NULL PRIMARY KEY,
                name               VARCHAR(512)  NOT NULL,
                status             VARCHAR(32)   NOT NULL,
                max_memory_bytes   BIGINT        NOT NULL DEFAULT 0,
                max_concurrent_queries INT       NOT NULL DEFAULT 0,
                max_queued_queries INT            NOT NULL DEFAULT 0,
                max_cpu_time_per_query_ms BIGINT  NOT NULL DEFAULT 0,
                max_scan_bytes_per_query BIGINT   NOT NULL DEFAULT 0,
                max_daily_query_count BIGINT      NOT NULL DEFAULT 0,
                max_daily_scan_bytes BIGINT       NOT NULL DEFAULT 0,
                max_storage_bytes  BIGINT         NOT NULL DEFAULT 0,
                allowed_catalogs   TEXT,
                allowed_schemas    TEXT,
                resource_pool      VARCHAR(255),
                properties         TEXT,
                created_at         TIMESTAMP      NOT NULL,
                updated_at         TIMESTAMP      NOT NULL,
                created_by         VARCHAR(255)
            )
            """;

    static final String DDL_MEMBERSHIPS = """
            CREATE TABLE IF NOT EXISTS tenant_user_memberships (
                user_principal     VARCHAR(255)  NOT NULL PRIMARY KEY,
                tenant_id          VARCHAR(255)  NOT NULL,
                role               VARCHAR(32)   NOT NULL
            )
            """;

    private static final String SQL_MERGE_TENANT =
            "INSERT INTO tenants (tenant_id, name, status, "
                    + "max_memory_bytes, max_concurrent_queries, max_queued_queries, "
                    + "max_cpu_time_per_query_ms, max_scan_bytes_per_query, "
                    + "max_daily_query_count, max_daily_scan_bytes, max_storage_bytes, "
                    + "allowed_catalogs, allowed_schemas, resource_pool, properties, "
                    + "created_at, updated_at, created_by) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (tenant_id) DO UPDATE SET "
                    + "name=EXCLUDED.name, status=EXCLUDED.status, "
                    + "max_memory_bytes=EXCLUDED.max_memory_bytes, "
                    + "max_concurrent_queries=EXCLUDED.max_concurrent_queries, "
                    + "max_queued_queries=EXCLUDED.max_queued_queries, "
                    + "max_cpu_time_per_query_ms=EXCLUDED.max_cpu_time_per_query_ms, "
                    + "max_scan_bytes_per_query=EXCLUDED.max_scan_bytes_per_query, "
                    + "max_daily_query_count=EXCLUDED.max_daily_query_count, "
                    + "max_daily_scan_bytes=EXCLUDED.max_daily_scan_bytes, "
                    + "max_storage_bytes=EXCLUDED.max_storage_bytes, "
                    + "allowed_catalogs=EXCLUDED.allowed_catalogs, "
                    + "allowed_schemas=EXCLUDED.allowed_schemas, "
                    + "resource_pool=EXCLUDED.resource_pool, "
                    + "properties=EXCLUDED.properties, "
                    + "updated_at=EXCLUDED.updated_at";

    private static final String SQL_GET_TENANT =
            "SELECT * FROM tenants WHERE tenant_id = ?";

    private static final String SQL_GET_ALL_ACTIVE =
            "SELECT * FROM tenants WHERE status = 'ACTIVE'";

    private static final String SQL_MERGE_MEMBERSHIP =
            "INSERT INTO tenant_user_memberships (user_principal, tenant_id, role) "
                    + "VALUES (?,?,?) "
                    + "ON CONFLICT (user_principal) DO UPDATE SET "
                    + "tenant_id=EXCLUDED.tenant_id, role=EXCLUDED.role";

    private static final String SQL_GET_MEMBERSHIP =
            "SELECT tenant_id, role FROM tenant_user_memberships WHERE user_principal = ?";

    private static final String SQL_DELETE_MEMBERSHIP =
            "DELETE FROM tenant_user_memberships WHERE tenant_id = ? AND user_principal = ?";

    // ── Fields ───────────────────────────────────────────────────────

    private final DataSource dataSource;

    public JdbcTenantStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Initializes the tenant and membership tables if they do not exist.
     * Call once during bootstrap.
     */
    public void initSchema() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate.lockMutation(conn);
            stmt.execute(DDL_TENANTS);
            stmt.execute(DDL_MEMBERSHIPS);
            org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate.initialize(conn);
            conn.commit();
            LOG.info("event=tenant_schema_initialized");
        } catch (SQLException e) {
            throw new MetastoreException("Failed to initialize tenant schema", e);
        }
    }

    /** Validates the exact current tenant membership and namespace on the publication connection. */
    public static void requireDeferredTenant(
            Connection connection, String principal, String tenantId,
            org.pangrova.spi.security.authz.DeferredAuthorizationScope scope) throws SQLException {
        requireDeferredTenant(connection, principal, tenantId, scope, table -> false);
    }

    /**
     * Validates tenant membership and namespace, exempting engine-owned resources
     * from the tenant's user-facing catalog/schema restriction.
     *
     * <p>Kernel-authored materialization writes name reserved internal tables in
     * the same catalog. Those tables are not tenant resources, so the tenant
     * namespace must not reject them, while every user-facing resource in the
     * same scope is still checked.</p>
     *
     * @param engineOwnedResource identifies tables created and owned by the engine
     */
    public static void requireDeferredTenant(
            Connection connection, String principal, String tenantId,
            org.pangrova.spi.security.authz.DeferredAuthorizationScope scope,
            java.util.function.Predicate<
                    org.pangrova.spi.connector.metadata.table.QualifiedTableName>
                    engineOwnedResource) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT t.* FROM tenants t JOIN tenant_user_memberships m ON m.tenant_id=t.tenant_id "
                        + "WHERE t.tenant_id=? AND m.user_principal=? AND t.status='ACTIVE'")) {
            statement.setString(1, tenantId);
            statement.setString(2, principal);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new org.pangrova.spi.security.AuthorizationDeniedException(
                            "Deferred subject no longer belongs to the exact active tenant");
                }
                Tenant tenant = mapTenant(result);
                for (var permission : scope.permissions()) {
                    var table = permission.table();
                    if (engineOwnedResource.test(table)) {
                        continue;
                    }
                    if ((!tenant.allowedCatalogs().isEmpty() && !tenant.allowedCatalogs().contains(table.catalog()))
                            || (!tenant.allowedSchemas().isEmpty()
                            && !tenant.allowedSchemas().contains(table.schema()))) {
                        throw new org.pangrova.spi.security.AuthorizationDeniedException(
                                "Deferred resource is outside the current tenant namespace");
                    }
                }
            }
        }
    }

    // ── TenantStore Implementation ───────────────────────────────────

    @Override
    public Optional<Tenant> getTenant(String tenantId) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_TENANT)) {
            ps.setString(1, tenantId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapTenant(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new MetastoreException("Failed to get tenant=" + tenantId, e);
        }
    }

    @Override
    public List<Tenant> getAllActiveTenants() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_ALL_ACTIVE);
             ResultSet rs = ps.executeQuery()) {
            List<Tenant> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapTenant(rs));
            }
            return Collections.unmodifiableList(result);
        } catch (SQLException e) {
            throw new MetastoreException("Failed to list active tenants", e);
        }
    }

    @Override
    public void createTenant(Tenant tenant) {
        Objects.requireNonNull(tenant, "tenant must not be null");
        mergeTenant(tenant);
        LOG.info("event=tenant_created tenantId={} name={}", tenant.tenantId(), tenant.name());
    }

    @Override
    public void updateTenant(Tenant tenant) {
        Objects.requireNonNull(tenant, "tenant must not be null");
        mergeTenant(tenant);
        LOG.info("event=tenant_updated tenantId={} status={}", tenant.tenantId(), tenant.status());
    }

    @Override
    public Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal) {
        Objects.requireNonNull(userPrincipal, "userPrincipal must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_MEMBERSHIP)) {
            ps.setString(1, userPrincipal);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String tenantId = rs.getString("tenant_id");
                TenantRole role = TenantRole.valueOf(rs.getString("role"));
                return getTenant(tenantId)
                        .map(t -> new ResolvedTenantContext(t, role));
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to get tenant for user=" + userPrincipal, e);
        }
    }

    @Override
    public void addUserToTenant(String tenantId, String userPrincipal, TenantRole role) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(userPrincipal, "userPrincipal must not be null");
        Objects.requireNonNull(role, "role must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_MERGE_MEMBERSHIP)) {
            ps.setString(1, userPrincipal);
            ps.setString(2, tenantId);
            ps.setString(3, role.name());
            ps.executeUpdate();
            LOG.info("event=tenant_user_added tenantId={} user={} role={}",
                    tenantId, userPrincipal, role);
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to add user=" + userPrincipal + " to tenant=" + tenantId, e);
        }
    }

    @Override
    public void removeUserFromTenant(String tenantId, String userPrincipal) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(userPrincipal, "userPrincipal must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_DELETE_MEMBERSHIP)) {
            ps.setString(1, tenantId);
            ps.setString(2, userPrincipal);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=tenant_user_removed tenantId={} user={}",
                        tenantId, userPrincipal);
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to remove user=" + userPrincipal + " from tenant=" + tenantId, e);
        }
    }

    // ── Private Helpers ──────────────────────────────────────────────

    private void mergeTenant(Tenant tenant) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_MERGE_TENANT)) {
            bindTenant(ps, tenant);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to merge tenant=" + tenant.tenantId(), e);
        }
    }

    private static void bindTenant(PreparedStatement ps, Tenant tenant) throws SQLException {
        ResourceQuota q = tenant.quota() != null ? tenant.quota() : EMPTY_QUOTA;
        ps.setString(PARAM_TENANT_ID, tenant.tenantId());
        ps.setString(PARAM_NAME, tenant.name());
        ps.setString(PARAM_STATUS, tenant.status().name());
        ps.setLong(PARAM_MAX_MEMORY_BYTES, q.maxMemoryBytes());
        ps.setInt(PARAM_MAX_CONCURRENT_QUERIES, q.maxConcurrentQueries());
        ps.setInt(PARAM_MAX_QUEUED_QUERIES, q.maxQueuedQueries());
        ps.setLong(PARAM_MAX_CPU_TIME_PER_QUERY_MS, q.maxCpuTimePerQueryMs());
        ps.setLong(PARAM_MAX_SCAN_BYTES_PER_QUERY, q.maxScanBytesPerQuery());
        ps.setLong(PARAM_MAX_DAILY_QUERY_COUNT, q.maxDailyQueryCount());
        ps.setLong(PARAM_MAX_DAILY_SCAN_BYTES, q.maxDailyScanBytes());
        ps.setLong(PARAM_MAX_STORAGE_BYTES, q.maxStorageBytes());
        ps.setString(PARAM_ALLOWED_CATALOGS, serializeList(tenant.allowedCatalogs()));
        ps.setString(PARAM_ALLOWED_SCHEMAS, serializeList(tenant.allowedSchemas()));
        ps.setString(PARAM_RESOURCE_POOL, tenant.resourcePool());
        ps.setString(PARAM_PROPERTIES, serializeMap(tenant.properties()));
        Timestamp now = Timestamp.from(Instant.now());
        ps.setTimestamp(PARAM_CREATED_AT, tenant.createdAt() != null
                ? Timestamp.from(tenant.createdAt()) : now);
        ps.setTimestamp(PARAM_UPDATED_AT, now);
        ps.setString(PARAM_CREATED_BY, tenant.createdBy());
    }

    private static Tenant mapTenant(ResultSet rs) throws SQLException {
        ResourceQuota quota = mapResourceQuota(rs);
        Timestamp createdAt = rs.getTimestamp("created_at");
        return new Tenant(
                rs.getString("tenant_id"), rs.getString("name"),
                TenantStatus.valueOf(rs.getString("status")), quota,
                deserializeList(rs.getString("allowed_catalogs")),
                deserializeList(rs.getString("allowed_schemas")),
                rs.getString("resource_pool"),
                deserializeMap(rs.getString("properties")),
                createdAt != null ? createdAt.toInstant() : null,
                rs.getString("created_by"));
    }

    private static ResourceQuota mapResourceQuota(ResultSet rs) throws SQLException {
        return new ResourceQuota(
                rs.getLong("max_memory_bytes"), rs.getInt("max_concurrent_queries"),
                rs.getInt("max_queued_queries"), rs.getLong("max_cpu_time_per_query_ms"),
                rs.getLong("max_scan_bytes_per_query"), rs.getLong("max_daily_query_count"),
                rs.getLong("max_daily_scan_bytes"), rs.getLong("max_storage_bytes"));
    }

    private static String serializeList(List<String> list) {
        if (list == null || list.isEmpty()) {
            return "[]";
        }
        try {
            return MAPPER.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("Failed to serialize list to JSON", e);
        }
    }

    private static List<String> deserializeList(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return MAPPER.readValue(json, LIST_TYPE);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("Failed to deserialize JSON to list", e);
        }
    }

    private static String serializeMap(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("Failed to serialize map to JSON", e);
        }
    }

    private static Map<String, String> deserializeMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("Failed to deserialize JSON to map", e);
        }
    }
}
