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
package org.pangrova.persistence.security.postgresql.tenant;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import java.util.stream.Stream;
import static org.assertj.core.api.Assertions.catchThrowable;


/**
 * Integration tests for JdbcTenantStore using PostgreSQL TestContainer.
 */
class JdbcTenantStoreTest {

    private DataSource dataSource;

    private static final ResourceQuota QUOTA = new ResourceQuota(
            10L * 1024 * 1024 * 1024, 10, 50, 3_600_000L,
            100L * 1024 * 1024 * 1024, 10_000L,
            1_000L * 1024 * 1024 * 1024, 100L * 1024 * 1024 * 1024);

    private JdbcTenantStore store;

    @BeforeEach
    void createSchema() throws SQLException {
        dataSource = org.pangrova.testing.testkit.postgres.MigratedPostgres.create();
        store = new JdbcTenantStore(dataSource);
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

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedCreationLeavesNoTenantQuotaOrProperties(Throwable failure, SQLException rollbackFailure) throws Exception {
        var failingStore = new JdbcTenantStore(JdbcMutationFault.afterMutation(dataSource,
                "INSERT INTO tenants ", failure, rollbackFailure));
        Throwable caught = catchThrowable(() -> failingStore.createTenant(buildTenant("failed", TenantStatus.ACTIVE, QUOTA)));
        assertThat(store.getTenant("failed")).isEmpty();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM tenants")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isZero();
        }
        assertFailure(caught, failure, rollbackFailure);
    }

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedUpdatePreservesTenantQuotaAndProperties(Throwable failure, SQLException rollbackFailure) {
        store.createTenant(new Tenant("existing", "Original", TenantStatus.ACTIVE, QUOTA,
                List.of("iceberg"), List.of("original"), "pool-1", Map.of("key", "original"),
                Instant.now(), "admin"));
        Tenant original = store.getTenant("existing").orElseThrow();
        ResourceQuota replacement = new ResourceQuota(1024, 1, 2, 3000, 4096, 5, 8192, 16384);
        Tenant changed = new Tenant("existing", "Changed", TenantStatus.SUSPENDED, replacement,
                List.of("paimon"), List.of("changed"), "pool-2", Map.of("key", "changed"),
                original.createdAt(), "other");
        var failingStore = new JdbcTenantStore(JdbcMutationFault.afterMutation(dataSource,
                "INSERT INTO tenants ", failure, rollbackFailure));
        Throwable caught = catchThrowable(() -> failingStore.updateTenant(changed));
        assertThat(store.getTenant("existing")).contains(original);
        assertFailure(caught, failure, rollbackFailure);
    }

    private static Stream<Arguments> transactionFailures() {
        return Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after mutation"), new IllegalStateException("after mutation"),
                new AssertionError("after mutation")).map(failure -> Arguments.of(failure,
                        failRollback ? new SQLException("rollback unavailable") : null)));
    }

    private static void assertFailure(Throwable caught, Throwable failure, SQLException rollbackFailure) {
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(org.pangrova.persistence.security.api.exception.TenantPersistenceException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
    }
    @Test
    void explicitZeroQuotaIsNotConfusedWithAbsentQuota() {
        var zero = new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0);
        store.createTenant(buildTenant("absent", TenantStatus.ACTIVE, null));
        store.createTenant(buildTenant("zero", TenantStatus.ACTIVE, zero));
        assertThat(store.getTenant("absent").orElseThrow().quota()).isNull();
        assertThat(store.getTenant("zero").orElseThrow().quota()).isEqualTo(zero);
    }

    @Test
    void staleTenantRemovalCannotEraseReassignedMembership() {
        store.createTenant(buildTenant("old", TenantStatus.ACTIVE, QUOTA));
        store.createTenant(buildTenant("new", TenantStatus.ACTIVE, QUOTA));
        store.addUserToTenant("old", "alice", TenantRole.MEMBER);
        store.addUserToTenant("new", "alice", TenantRole.ADMIN);
        store.removeUserFromTenant("old", "alice");
        assertThat(store.getTenantForUser("alice")).hasValueSatisfying(context -> {
            assertThat(context.tenantId()).isEqualTo("new");
            assertThat(context.roleName()).isEqualTo("ADMIN");
        });
    }

    @Test
    void canonicalInitializationDoesNotCreateParallelTenantTables() throws Exception {
        try (var connection = dataSource.getConnection(); var query = connection.createStatement();
                var result = query.executeQuery("SELECT to_regclass('tenant'), to_regclass('tenant_quota'), "
                        + "to_regclass('tenant_property'), to_regclass('tenant_membership')")) {
            assertThat(result.next()).isTrue();
            for (int column = 1; column <= 4; column++) assertThat(result.getString(column)).isNull();
        }
    }

    @Test
    void storeCannotInventItsOwnSchema() throws Exception {
        var empty = org.pangrova.testing.testkit.postgres.SharedPostgres.isolatedDataSource();
        var uninitialized = new JdbcTenantStore(empty);
        Throwable failure = catchThrowable(() -> uninitialized.createTenant(
                buildTenant("uninitialized", TenantStatus.ACTIVE, QUOTA)));
        assertThat(failure).isInstanceOf(org.pangrova.persistence.security.api.exception.TenantPersistenceException.class);
        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("42P01");
        try (var connection = empty.getConnection(); var query = connection.createStatement();
                var result = query.executeQuery("SELECT to_regclass('tenants')")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isNull();
        }
    }

    @Test
    void canonicalInitializationFencesTenantMutationWithoutStoreInitialization() throws Exception {
        long before = authorizationRevision();
        store.createTenant(buildTenant("authority", TenantStatus.ACTIVE, QUOTA));
        assertThat(authorizationRevision()).isGreaterThan(before);
        long afterTenant = authorizationRevision();
        store.addUserToTenant("authority", "alice", TenantRole.MEMBER);
        assertThat(authorizationRevision()).isGreaterThan(afterTenant);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThat(catchThrowable(() -> statement.execute("TRUNCATE tenants")))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be truncated");
        }
    }

    private long authorizationRevision() throws SQLException {
        try (var connection = dataSource.getConnection(); var query = connection.createStatement();
                var result = query.executeQuery("SELECT revision FROM pangrova_authorization_revision")) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }

    private static Tenant buildTenant(String tenantId, TenantStatus status, ResourceQuota quota) {
        return new Tenant(tenantId, "Engineering", status, quota,
                List.of("iceberg"), List.of("default"), "pool-1",
                Map.of(), Instant.now(), "admin");
    }
}
