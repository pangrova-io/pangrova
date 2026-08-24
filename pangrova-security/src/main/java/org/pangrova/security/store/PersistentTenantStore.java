package org.pangrova.security.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.store.ResolvedTenantContext;
import org.pangrova.security.tenant.store.TenantStore;

/**
 * Tenant persistence on the shared SQL state (SI-4). Stores tenant records, per-tenant resource
 * quotas, arbitrary properties, and user-to-tenant membership. Selected by tenant bootstrap when
 * tenancy is enabled so quotas and membership survive restart/failover.
 */
public final class PersistentTenantStore extends PersistentStoreSupport implements TenantStore {

    private static final String TENANT_COLS =
            "t.tenant_id, t.name, t.status, t.resource_pool, "
            + "t.allowed_catalogs, t.allowed_schemas, t.created_at, t.created_by, "
            + "q.max_memory_bytes, q.max_concurrent, q.max_queued_queries, "
            + "q.max_cpu_time_per_query_ms, q.max_scan_bytes_per_query, "
            + "q.max_daily_query_count, q.max_daily_scan_bytes, q.max_storage_bytes";

    private static final String TENANT_JOIN =
            "FROM tenant t LEFT JOIN tenant_quota q ON t.tenant_id = q.tenant_id ";

    public PersistentTenantStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Optional<Tenant> getTenant(String tenantId) {
        String sql = "SELECT " + TENANT_COLS + " " + TENANT_JOIN + "WHERE t.tenant_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tenantId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(withProperties(conn, mapTenant(rs)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading tenant " + tenantId, e);
        }
    }

    @Override
    public List<Tenant> getAllActiveTenants() {
        String sql = "SELECT " + TENANT_COLS + " " + TENANT_JOIN + "WHERE t.status = 'ACTIVE'";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<Tenant> tenants = new ArrayList<>();
            while (rs.next()) {
                tenants.add(withProperties(conn, mapTenant(rs)));
            }
            return tenants;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading active tenants", e);
        }
    }

    @Override
    public void createTenant(Tenant tenant) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                insertTenantRow(conn, tenant);
                upsertQuotaRow(conn, tenant);
                replaceProperties(conn, tenant);
                conn.commit();
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed creating tenant " + tenant.tenantId(), e);
        }
    }

    @Override
    public void updateTenant(Tenant tenant) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                updateTenantRow(conn, tenant);
                upsertQuotaRow(conn, tenant);
                replaceProperties(conn, tenant);
                conn.commit();
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed updating tenant " + tenant.tenantId(), e);
        }
    }

    @Override
    public Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal) {
        String sql = "SELECT " + TENANT_COLS + ", m.role "
                + "FROM tenant_membership m "
                + "JOIN tenant t ON m.tenant_id = t.tenant_id "
                + "LEFT JOIN tenant_quota q ON t.tenant_id = q.tenant_id "
                + "WHERE m.user_principal = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userPrincipal);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                TenantRole role = TenantRole.valueOf(rs.getString("role"));
                Tenant tenant = withProperties(conn, mapTenant(rs));
                return Optional.of(new ResolvedTenantContext(tenant, role));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading tenant for user " + userPrincipal, e);
        }
    }

    @Override
    public void addUserToTenant(String tenantId, String userPrincipal, TenantRole role) {
        try (Connection conn = dataSource.getConnection()) {
            upsertMembershipRow(conn, tenantId, userPrincipal, role);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed adding user " + userPrincipal + " to tenant " + tenantId, e);
        }
    }

    @Override
    public void removeUserFromTenant(String tenantId, String userPrincipal) {
        String sql = "DELETE FROM tenant_membership WHERE user_principal = ? AND tenant_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userPrincipal);
            ps.setString(2, tenantId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed removing user " + userPrincipal + " from tenant " + tenantId, e);
        }
    }

    // --- private helpers ---

    private Tenant mapTenant(ResultSet rs) throws SQLException {
        String tenantId = rs.getString("tenant_id");
        TenantStatus status = TenantStatus.valueOf(rs.getString("status"));
        List<String> catalogs = decodeList(rs.getString("allowed_catalogs"));
        List<String> schemas = decodeList(rs.getString("allowed_schemas"));
        Timestamp ts = rs.getTimestamp("created_at");
        Instant createdAt = ts != null ? ts.toInstant() : Instant.EPOCH;

        long maxMemBytes = rs.getLong("max_memory_bytes");
        ResourceQuota quota = rs.wasNull() ? null : new ResourceQuota(
                maxMemBytes,
                rs.getInt("max_concurrent"),
                rs.getInt("max_queued_queries"),
                rs.getLong("max_cpu_time_per_query_ms"),
                rs.getLong("max_scan_bytes_per_query"),
                rs.getLong("max_daily_query_count"),
                rs.getLong("max_daily_scan_bytes"),
                rs.getLong("max_storage_bytes"));

        return new Tenant(tenantId, rs.getString("name"), status, quota,
                catalogs, schemas, rs.getString("resource_pool"), Map.of(), createdAt,
                rs.getString("created_by"));
    }

    private Tenant withProperties(Connection conn, Tenant tenant) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT prop_key, prop_value FROM tenant_property WHERE tenant_id = ?")) {
            ps.setString(1, tenant.tenantId());
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> props = new HashMap<>();
                while (rs.next()) {
                    props.put(rs.getString("prop_key"), rs.getString("prop_value"));
                }
                if (props.isEmpty()) {
                    return tenant;
                }
                return new Tenant(tenant.tenantId(), tenant.name(), tenant.status(), tenant.quota(),
                        tenant.allowedCatalogs(), tenant.allowedSchemas(), tenant.resourcePool(),
                        props, tenant.createdAt(), tenant.createdBy());
            }
        }
    }

    private void insertTenantRow(Connection conn, Tenant tenant) throws SQLException {
        String sql = "INSERT INTO tenant (tenant_id, name, status, resource_pool, "
                + "allowed_catalogs, allowed_schemas, created_at, created_by) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tenant.tenantId());
            ps.setString(2, tenant.name());
            ps.setString(3, tenant.status().name());
            ps.setString(4, tenant.resourcePool());
            ps.setString(5, encodeList(tenant.allowedCatalogs()));
            ps.setString(6, encodeList(tenant.allowedSchemas()));
            Instant ci = tenant.createdAt() != null ? tenant.createdAt() : Instant.now();
            ps.setTimestamp(7, Timestamp.from(ci));
            ps.setString(8, tenant.createdBy());
            ps.executeUpdate();
        }
    }

    private void updateTenantRow(Connection conn, Tenant tenant) throws SQLException {
        String sql = "UPDATE tenant SET name = ?, status = ?, resource_pool = ?, "
                + "allowed_catalogs = ?, allowed_schemas = ? WHERE tenant_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tenant.name());
            ps.setString(2, tenant.status().name());
            ps.setString(3, tenant.resourcePool());
            ps.setString(4, encodeList(tenant.allowedCatalogs()));
            ps.setString(5, encodeList(tenant.allowedSchemas()));
            ps.setString(6, tenant.tenantId());
            int rows = ps.executeUpdate();
            if (rows == 0) {
                throw new IllegalStateException("Tenant not found for update: " + tenant.tenantId());
            }
        }
    }

    private void upsertQuotaRow(Connection conn, Tenant tenant) throws SQLException {
        if (tenant.quota() == null) {
            return;
        }
        ResourceQuota q = tenant.quota();
        String update = "UPDATE tenant_quota SET max_memory_bytes=?, max_concurrent=?, "
                + "max_queued_queries=?, max_cpu_time_per_query_ms=?, max_scan_bytes_per_query=?, "
                + "max_daily_query_count=?, max_daily_scan_bytes=?, max_storage_bytes=? "
                + "WHERE tenant_id=?";
        try (PreparedStatement ps = conn.prepareStatement(update)) {
            ps.setLong(1, q.maxMemoryBytes());
            ps.setInt(2, q.maxConcurrentQueries());
            ps.setInt(3, q.maxQueuedQueries());
            ps.setLong(4, q.maxCpuTimePerQueryMs());
            ps.setLong(5, q.maxScanBytesPerQuery());
            ps.setLong(6, q.maxDailyQueryCount());
            ps.setLong(7, q.maxDailyScanBytes());
            ps.setLong(8, q.maxStorageBytes());
            ps.setString(9, tenant.tenantId());
            if (ps.executeUpdate() == 0) {
                String insert = "INSERT INTO tenant_quota (tenant_id, max_memory_bytes, "
                        + "max_concurrent, max_queued_queries, max_cpu_time_per_query_ms, "
                        + "max_scan_bytes_per_query, max_daily_query_count, max_daily_scan_bytes, "
                        + "max_storage_bytes, cpu_weight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0)";
                try (PreparedStatement ins = conn.prepareStatement(insert)) {
                    ins.setString(1, tenant.tenantId());
                    ins.setLong(2, q.maxMemoryBytes());
                    ins.setInt(3, q.maxConcurrentQueries());
                    ins.setInt(4, q.maxQueuedQueries());
                    ins.setLong(5, q.maxCpuTimePerQueryMs());
                    ins.setLong(6, q.maxScanBytesPerQuery());
                    ins.setLong(7, q.maxDailyQueryCount());
                    ins.setLong(8, q.maxDailyScanBytes());
                    ins.setLong(9, q.maxStorageBytes());
                    ins.executeUpdate();
                }
            }
        }
    }

    private void upsertMembershipRow(Connection conn, String tenantId,
                                     String userPrincipal, TenantRole role) throws SQLException {
        String update = "UPDATE tenant_membership SET tenant_id=?, role=? WHERE user_principal=?";
        try (PreparedStatement ps = conn.prepareStatement(update)) {
            ps.setString(1, tenantId);
            ps.setString(2, role.name());
            ps.setString(3, userPrincipal);
            if (ps.executeUpdate() == 0) {
                try (PreparedStatement ins = conn.prepareStatement(
                        "INSERT INTO tenant_membership (user_principal, tenant_id, role) "
                                + "VALUES (?, ?, ?)")) {
                    ins.setString(1, userPrincipal);
                    ins.setString(2, tenantId);
                    ins.setString(3, role.name());
                    ins.executeUpdate();
                }
            }
        }
    }

    private void replaceProperties(Connection conn, Tenant tenant) throws SQLException {
        try (PreparedStatement del = conn.prepareStatement(
                "DELETE FROM tenant_property WHERE tenant_id = ?")) {
            del.setString(1, tenant.tenantId());
            del.executeUpdate();
        }
        if (tenant.properties().isEmpty()) {
            return;
        }
        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO tenant_property (tenant_id, prop_key, prop_value) VALUES (?, ?, ?)")) {
            for (Map.Entry<String, String> entry : tenant.properties().entrySet()) {
                ins.setString(1, tenant.tenantId());
                ins.setString(2, entry.getKey());
                ins.setString(3, entry.getValue());
                ins.addBatch();
            }
            ins.executeBatch();
        }
    }

    private static String encodeList(List<String> items) {
        return items == null || items.isEmpty() ? "" : String.join(",", items);
    }

    private static List<String> decodeList(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return List.of();
        }
        return List.of(encoded.split(","));
    }
}
