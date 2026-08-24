/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.security.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.store.ResolvedTenantContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Integration tests for PersistentTenantStore using PostgreSQL TestContainer.
 */
class PersistentTenantStoreTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() {
        PG.start();
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        new PersistentTenantStore(ds).initSchema();
    }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private static DataSource dataSource;

    private static final ResourceQuota QUOTA = new ResourceQuota(
            10L * 1024 * 1024 * 1024, 10, 50, 3_600_000L,
            100L * 1024 * 1024 * 1024, 10_000L,
            1_000L * 1024 * 1024 * 1024, 100L * 1024 * 1024 * 1024);

    private PersistentTenantStore store;

    @BeforeEach
    void cleanTables() throws SQLException {
        if (dataSource == null) {
            org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
            ds.setUrl(PG.getJdbcUrl());
            ds.setUser(PG.getUsername());
            ds.setPassword(PG.getPassword());
            dataSource = ds;
        }
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM tenant_membership");
            stmt.execute("DELETE FROM tenant_property");
            stmt.execute("DELETE FROM tenant_quota");
            stmt.execute("DELETE FROM tenant");
        }
        store = new PersistentTenantStore(dataSource);
    }

    @Test
    void createAndGetTenantRoundtrip() {
        store.createTenant(buildTenant("t-001", TenantStatus.ACTIVE, QUOTA));

        Tenant t = store.getTenant("t-001").orElseThrow();
        assertThat(t.tenantId()).isEqualTo("t-001");
        assertThat(t.name()).isEqualTo("Engineering");
        assertThat(t.status()).isEqualTo(TenantStatus.ACTIVE);
        assertThat(t.allowedCatalogs()).containsExactly("iceberg");
        assertThat(t.allowedSchemas()).containsExactly("default");
        assertThat(t.resourcePool()).isEqualTo("pool-1");
        assertThat(t.quota()).isNotNull();
        assertThat(t.quota().maxMemoryBytes()).isEqualTo(QUOTA.maxMemoryBytes());
        assertThat(t.quota().maxConcurrentQueries()).isEqualTo(QUOTA.maxConcurrentQueries());
        assertThat(t.quota().maxQueuedQueries()).isEqualTo(QUOTA.maxQueuedQueries());
        assertThat(t.quota().maxStorageBytes()).isEqualTo(QUOTA.maxStorageBytes());
    }

    @Test
    void getTenantReturnsEmptyForUnknownId() {
        assertThat(store.getTenant("unknown")).isEmpty();
    }

    @Test
    void getAllActiveTenantsFiltersNonActive() {
        store.createTenant(buildTenant("t-active", TenantStatus.ACTIVE, QUOTA));
        store.createTenant(buildTenant("t-suspended", TenantStatus.SUSPENDED, null));
        store.createTenant(buildTenant("t-deleted", TenantStatus.DELETED, null));

        List<Tenant> active = store.getAllActiveTenants();
        assertThat(active).hasSize(1);
        assertThat(active.get(0).tenantId()).isEqualTo("t-active");
    }

    @Test
    void updateTenantPersistsChanges() {
        store.createTenant(buildTenant("t-001", TenantStatus.ACTIVE, QUOTA));

        ResourceQuota newQuota = new ResourceQuota(
                20L * 1024 * 1024 * 1024, 20, 100, 0L, 0L, 0L, 0L, 0L);
        Tenant updated = new Tenant("t-001", "Engineering Updated", TenantStatus.SUSPENDED,
                newQuota, List.of("delta"), List.of("schema2"), "pool-2",
                Map.of("env", "staging"), Instant.now(), "admin");
        store.updateTenant(updated);

        Tenant loaded = store.getTenant("t-001").orElseThrow();
        assertThat(loaded.name()).isEqualTo("Engineering Updated");
        assertThat(loaded.status()).isEqualTo(TenantStatus.SUSPENDED);
        assertThat(loaded.allowedCatalogs()).containsExactly("delta");
        assertThat(loaded.quota().maxConcurrentQueries()).isEqualTo(20);
        assertThat(loaded.properties()).containsEntry("env", "staging");
    }

    @Test
    void propertiesRoundtrip() {
        Tenant tenant = new Tenant("t-props", "Props Tenant", TenantStatus.ACTIVE,
                QUOTA, List.of(), List.of(), null,
                Map.of("key1", "val1", "key2", "val2"), Instant.now(), "admin");
        store.createTenant(tenant);

        Tenant loaded = store.getTenant("t-props").orElseThrow();
        assertThat(loaded.properties())
                .containsEntry("key1", "val1")
                .containsEntry("key2", "val2");
    }

    @Test
    void addUserAndGetTenantForUser() {
        store.createTenant(buildTenant("t-001", TenantStatus.ACTIVE, QUOTA));
        store.addUserToTenant("t-001", "alice@example.com", TenantRole.MEMBER);

        Optional<ResolvedTenantContext> ctx = store.getTenantForUser("alice@example.com");
        assertThat(ctx).isPresent();
        assertThat(ctx.get().tenantId()).isEqualTo("t-001");
        assertThat(ctx.get().roleName()).isEqualTo("MEMBER");
        assertThat(ctx.get().isActive()).isTrue();
    }

    @Test
    void getTenantForUserReturnsEmptyForUnknownUser() {
        assertThat(store.getTenantForUser("nobody@example.com")).isEmpty();
    }

    @Test
    void removeUserFromTenant() {
        store.createTenant(buildTenant("t-001", TenantStatus.ACTIVE, QUOTA));
        store.addUserToTenant("t-001", "alice@example.com", TenantRole.OWNER);
        store.removeUserFromTenant("t-001", "alice@example.com");

        assertThat(store.getTenantForUser("alice@example.com")).isEmpty();
    }

    @Test
    void addUserToTenantUpserts() {
        store.createTenant(buildTenant("t-001", TenantStatus.ACTIVE, QUOTA));
        store.createTenant(buildTenant("t-002", TenantStatus.ACTIVE, QUOTA));
        store.addUserToTenant("t-001", "alice@example.com", TenantRole.MEMBER);
        store.addUserToTenant("t-002", "alice@example.com", TenantRole.ADMIN);

        Optional<ResolvedTenantContext> ctx = store.getTenantForUser("alice@example.com");
        assertThat(ctx).isPresent();
        assertThat(ctx.get().tenantId()).isEqualTo("t-002");
        assertThat(ctx.get().roleName()).isEqualTo("ADMIN");
    }

    @Test
    void nullQuotaPreservedOnRoundtrip() {
        store.createTenant(buildTenant("t-noquota", TenantStatus.ACTIVE, null));

        Tenant loaded = store.getTenant("t-noquota").orElseThrow();
        assertThat(loaded.quota()).isNull();
    }

    private static Tenant buildTenant(String tenantId, TenantStatus status, ResourceQuota quota) {
        return new Tenant(tenantId, "Engineering", status, quota,
                List.of("iceberg"), List.of("default"), "pool-1",
                Map.of(), Instant.now(), "admin");
    }
}
