package org.pangrova.metadata.tenant;

import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantRole;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcTenantStoreTest {

    private JdbcTenantStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcTenantStore(SharedPostgresContainer.dataSource());
        store.initSchema();
    }

    @Test
    void createAndGetTenantRoundTrip() {
        Tenant tenant = createTenant("t1", "Acme Corp", TenantStatus.ACTIVE, 10);
        store.createTenant(tenant);

        Optional<Tenant> loaded = store.getTenant("t1");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().tenantId()).isEqualTo("t1");
        assertThat(loaded.get().name()).isEqualTo("Acme Corp");
        assertThat(loaded.get().status()).isEqualTo(TenantStatus.ACTIVE);
        assertThat(loaded.get().quota().maxConcurrentQueries()).isEqualTo(10);
    }

    @Test
    void createAndUpdatePopulateRequiredUpdatedAt() throws SQLException {
        Tenant tenant = createTenant("timestamp-tenant", "Before", TenantStatus.ACTIVE, 10);
        store.createTenant(tenant);
        Instant insertedAt = readUpdatedAt("timestamp-tenant");
        assertThat(insertedAt).isNotNull();

        store.updateTenant(new Tenant("timestamp-tenant", "After", TenantStatus.ACTIVE,
                tenant.quota(), tenant.allowedCatalogs(), tenant.allowedSchemas(),
                tenant.resourcePool(), tenant.properties(), tenant.createdAt(), tenant.createdBy()));
        assertThat(readUpdatedAt("timestamp-tenant")).isAfterOrEqualTo(insertedAt);
    }

    @Test
    void getTenantReturnsEmptyForMissing() {
        assertThat(store.getTenant("nonexistent")).isEmpty();
    }

    @Test
    void updateTenantOverwritesExisting() {
        Tenant original = createTenant("t2", "Beta Inc", TenantStatus.ACTIVE, 5);
        store.createTenant(original);

        Tenant updated = new Tenant("t2", "Beta Inc Updated", TenantStatus.SUSPENDED,
                original.quota(), original.allowedCatalogs(), original.allowedSchemas(),
                original.resourcePool(), original.properties(),
                original.createdAt(), original.createdBy());
        store.updateTenant(updated);

        Optional<Tenant> loaded = store.getTenant("t2");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().name()).isEqualTo("Beta Inc Updated");
        assertThat(loaded.get().status()).isEqualTo(TenantStatus.SUSPENDED);
    }

    @Test
    void getAllActiveTenantsFiltersNonActive() {
        store.createTenant(createTenant("active-1", "A1", TenantStatus.ACTIVE, 5));
        store.createTenant(createTenant("active-2", "A2", TenantStatus.ACTIVE, 5));
        store.createTenant(createTenant("suspended-1", "S1", TenantStatus.SUSPENDED, 5));
        store.createTenant(createTenant("deleted-1", "D1", TenantStatus.DELETED, 5));

        List<Tenant> active = store.getAllActiveTenants();
        assertThat(active).hasSize(2);
        assertThat(active).extracting(Tenant::tenantId)
                .containsExactlyInAnyOrder("active-1", "active-2");
    }

    @Test
    void addUserAndResolveTenant() {
        Tenant tenant = createTenant("t3", "Gamma LLC", TenantStatus.ACTIVE, 10);
        store.createTenant(tenant);
        store.addUserToTenant("t3", "alice", TenantRole.MEMBER);

        Optional<ResolvedTenantContext> ctx = store.getTenantForUser("alice");
        assertThat(ctx).isPresent();
        assertThat(ctx.get().tenantId()).isEqualTo("t3");
        assertThat(ctx.get().role()).isEqualTo(TenantRole.MEMBER);
        assertThat(ctx.get().isActive()).isTrue();
    }

    @Test
    void getTenantForUserReturnsEmptyForUnknown() {
        assertThat(store.getTenantForUser("unknown-user")).isEmpty();
    }

    @Test
    void removeUserFromTenant() {
        store.createTenant(createTenant("t4", "Delta Co", TenantStatus.ACTIVE, 5));
        store.addUserToTenant("t4", "bob", TenantRole.ADMIN);
        assertThat(store.getTenantForUser("bob")).isPresent();

        store.removeUserFromTenant("t4", "bob");
        assertThat(store.getTenantForUser("bob")).isEmpty();
    }

    @Test
    void addUserToTenantUpsertOverwritesRole() {
        store.createTenant(createTenant("t5", "Echo Ltd", TenantStatus.ACTIVE, 5));
        store.addUserToTenant("t5", "carol", TenantRole.MEMBER);
        store.addUserToTenant("t5", "carol", TenantRole.OWNER);

        Optional<ResolvedTenantContext> ctx = store.getTenantForUser("carol");
        assertThat(ctx).isPresent();
        assertThat(ctx.get().role()).isEqualTo(TenantRole.OWNER);
    }

    @Test
    void tenantWithAllFieldsPersistsCorrectly() {
        ResourceQuota quota = new ResourceQuota(
                1024L * 1024 * 1024, 50, 100,
                30_000L, 10_000_000_000L,
                10_000L, 100_000_000_000L, 500_000_000_000L);
        Instant now = Instant.now();
        Tenant tenant = new Tenant("t-full", "Full Tenant", TenantStatus.ACTIVE,
                quota,
                List.of("catalog_a", "catalog_b"),
                List.of("schema_x"),
                "pool-premium",
                Map.of("env", "prod", "region", "us-east-1"),
                now, "admin-user");
        store.createTenant(tenant);

        Tenant loaded = store.getTenant("t-full").orElseThrow();
        assertThat(loaded.quota().maxMemoryBytes()).isEqualTo(1024L * 1024 * 1024);
        assertThat(loaded.quota().maxConcurrentQueries()).isEqualTo(50);
        assertThat(loaded.quota().maxStorageBytes()).isEqualTo(500_000_000_000L);
        assertThat(loaded.allowedCatalogs()).containsExactly("catalog_a", "catalog_b");
        assertThat(loaded.allowedSchemas()).containsExactly("schema_x");
        assertThat(loaded.resourcePool()).isEqualTo("pool-premium");
        assertThat(loaded.properties()).containsEntry("env", "prod");
        assertThat(loaded.createdBy()).isEqualTo("admin-user");
    }

    @Test
    void suspendedTenantResolvedAsInactive() {
        store.createTenant(createTenant("t-sus", "Suspended", TenantStatus.SUSPENDED, 5));
        store.addUserToTenant("t-sus", "dave", TenantRole.MEMBER);

        ResolvedTenantContext ctx = store.getTenantForUser("dave").orElseThrow();
        assertThat(ctx.isActive()).isFalse();
        assertThat(ctx.isSuspended()).isTrue();
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private static Tenant createTenant(String id, String name,
                                       TenantStatus status, int maxConcurrent) {
        ResourceQuota quota = new ResourceQuota(
                0, maxConcurrent, 0, 0, 0, 0, 0, 0);
        return new Tenant(id, name, status, quota,
                List.of(), List.of(), null, Map.of(),
                Instant.now(), "test-user");
    }

    private static Instant readUpdatedAt(String tenantId) throws SQLException {
        try (var connection = SharedPostgresContainer.dataSource().getConnection();
             var statement = connection.prepareStatement(
                     "SELECT updated_at FROM tenants WHERE tenant_id=?")) {
            statement.setString(1, tenantId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getTimestamp(1).toInstant();
            }
        }
    }
}
