package org.pangrova.testing.integration.e2e.security;

import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantRole;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.core.tenant.QuotaCheckResult;
import org.pangrova.security.core.tenant.QuotaChecker;
import org.pangrova.security.core.tenant.TenantUsage;
import org.pangrova.security.core.tenant.TenantResolver;
import org.pangrova.security.core.tenant.TenantLifecycleManager;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.types.value.TypedRow;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for tenant resource quota enforcement, cross-tenant data isolation,
 * and tenant lifecycle management.
 *
 * <p>Validates: per-tenant resource quotas, cross-tenant table isolation,
 * quota enforcement (query rejected when exceeded), and tenant creation/deletion lifecycle.
 *
 * <p>Validates: Requirements R12.5
 */
@DisplayName("Tenant Resource Quota E2E — Isolation, Quota, Lifecycle")
class TenantResourceQuotaE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTestTable() {
        tableName = "quota_test_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                "id BIGINT, name VARCHAR, value BIGINT");
        String rows = java.util.stream.IntStream.rangeClosed(1, 20)
                .mapToObj(i -> String.format("(%d, 'item_%d', %d)", i, i, i * 10))
                .collect(java.util.stream.Collectors.joining(", "));
        sqlRunner.execute("INSERT INTO " + FQN_PREFIX + tableName
                + " (id, name, value) VALUES " + rows);
    }

    // -----------------------------------------------------------------------
    // Basic Query Execution (existing tests)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Basic Query Execution")
    class BasicQueryExecution {

        @Test
        @DisplayName("Single query within quota succeeds")
        void singleQuery_withinQuota_succeeds() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + tableName);

            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(20L);
        }

        @Test
        @DisplayName("Concurrent queries within quota all succeed")
        void concurrentQueries_withinQuota_allSucceed() throws Exception {
            int queryCount = 4;
            ExecutorService executor = Executors.newFixedThreadPool(queryCount);
            AtomicInteger successes = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();

            try {
                for (int i = 0; i < queryCount; i++) {
                    final int idx = i;
                    futures.add(executor.submit(() -> {
                        List<TypedRow> rows = sqlRunner.execute(
                                "SELECT SUM(value) AS total FROM " + FQN_PREFIX + tableName
                                        + " WHERE id > " + idx);
                        if (!rows.isEmpty()) {
                            successes.incrementAndGet();
                        }
                    }));
                }

                for (Future<?> f : futures) {
                    f.get();
                }
            } finally {
                executor.shutdown();
            }

            assertThat(successes.get()).isEqualTo(queryCount);
        }

        @Test
        @DisplayName("Sequential queries accumulate correct results")
        void sequentialQueries_accumulateCorrectResults() {
            List<TypedRow> countResult = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + tableName);
            long count = ((Number) countResult.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(20L);

            List<TypedRow> sumResult = sqlRunner.execute(
                    "SELECT SUM(value) AS total FROM " + FQN_PREFIX + tableName);
            long total = ((Number) sumResult.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(2100L);

            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT id, value FROM " + FQN_PREFIX + tableName
                            + " WHERE value > 150 ORDER BY id");
            assertThat(filtered).hasSize(5);
        }
    }

    // -----------------------------------------------------------------------
    // Cross-Tenant Data Isolation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Cross-Tenant Data Isolation")
    class CrossTenantIsolation {

        @Test
        @DisplayName("Tenant A cannot see tenant B's tables via catalog restriction")
        void tenantA_cannotSeeTenantB_tables() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            Tenant tenantA = createTenant("tenant_a", "Tenant A",
                    List.of(CATALOG), List.of("schema_a"));
            Tenant tenantB = createTenant("tenant_b", "Tenant B",
                    List.of(CATALOG), List.of("schema_b"));
            store.createTenant(tenantA);
            store.createTenant(tenantB);
            store.addUserToTenant("tenant_a", "alice", TenantRole.MEMBER);
            store.addUserToTenant("tenant_b", "bob", TenantRole.MEMBER);

            TenantResolver resolver = new TenantResolver(store);
            UserIdentity alice = new UserIdentity(
                    "alice", Set.of("team_a"), Map.of(), Instant.now());

            Optional<ResolvedTenantContext> ctx = resolver.resolve(alice);
            assertThat(ctx).isPresent();
            assertThat(ctx.get().tenant().tenantId()).isEqualTo("tenant_a");
            // Tenant A's allowed schemas do not include schema_b
            assertThat(ctx.get().tenant().allowedSchemas()).contains("schema_a");
            assertThat(ctx.get().tenant().allowedSchemas()).doesNotContain("schema_b");
        }

        @Test
        @DisplayName("Tenant B resolver returns tenant B context, not tenant A")
        void tenantB_resolverReturnsTenantB() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            store.createTenant(createTenant("tenant_a", "Tenant A",
                    List.of(CATALOG), List.of("schema_a")));
            store.createTenant(createTenant("tenant_b", "Tenant B",
                    List.of(CATALOG), List.of("schema_b")));
            store.addUserToTenant("tenant_a", "alice", TenantRole.MEMBER);
            store.addUserToTenant("tenant_b", "bob", TenantRole.MEMBER);

            TenantResolver resolver = new TenantResolver(store);
            UserIdentity bob = new UserIdentity(
                    "bob", Set.of("team_b"), Map.of(), Instant.now());

            Optional<ResolvedTenantContext> ctx = resolver.resolve(bob);
            assertThat(ctx).isPresent();
            assertThat(ctx.get().tenant().tenantId()).isEqualTo("tenant_b");
            assertThat(ctx.get().tenant().allowedSchemas()).doesNotContain("schema_a");
        }

        @Test
        @DisplayName("User not assigned to any tenant gets empty resolution")
        void unassignedUser_getsEmptyResolution() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            store.createTenant(createTenant("tenant_a", "Tenant A",
                    List.of(CATALOG), List.of("schema_a")));

            TenantResolver resolver = new TenantResolver(store);
            UserIdentity stranger = new UserIdentity(
                    "stranger", Set.of("nobody"), Map.of(), Instant.now());

            Optional<ResolvedTenantContext> ctx = resolver.resolve(stranger);
            assertThat(ctx).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Quota Enforcement
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Quota Enforcement")
    class QuotaEnforcement {

        @Test
        @DisplayName("Query rejected when concurrent query quota exceeded")
        void queryRejected_whenConcurrentQuotaExceeded() {
            ResourceQuota quota = new ResourceQuota(
                    10737418240L, 2, 100, 3600000L,
                    107374182400L, 10000L, 1099511627776L, 1099511627776L);
            TenantUsage usage = new TenantUsage(2, 0, 0, 0);

            QuotaChecker checker = new QuotaChecker();
            QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);

            assertThat(result.passed()).isFalse();
            assertThat(result.violations()).anyMatch(v -> v.contains("concurrent queries"));
        }

        @Test
        @DisplayName("Query passes when within concurrent query quota")
        void queryPasses_whenWithinConcurrentQuota() {
            ResourceQuota quota = new ResourceQuota(
                    10737418240L, 10, 100, 3600000L,
                    107374182400L, 10000L, 1099511627776L, 1099511627776L);
            TenantUsage usage = new TenantUsage(5, 0, 0, 0);

            QuotaChecker checker = new QuotaChecker();
            QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);

            assertThat(result.passed()).isTrue();
            assertThat(result.violations()).isEmpty();
        }

        @Test
        @DisplayName("Query rejected when daily query count quota exceeded")
        void queryRejected_whenDailyQueryCountExceeded() {
            ResourceQuota quota = new ResourceQuota(
                    10737418240L, 10, 100, 3600000L,
                    107374182400L, 100L, 1099511627776L, 1099511627776L);
            TenantUsage usage = new TenantUsage(0, 0, 100, 0);

            QuotaChecker checker = new QuotaChecker();
            QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);

            assertThat(result.passed()).isFalse();
            assertThat(result.violations()).anyMatch(v -> v.contains("daily query count"));
        }

        @Test
        @DisplayName("Zero concurrent quota rejects any query")
        void zeroConcurrentQuota_rejectsAnyQuery() {
            ResourceQuota quota = new ResourceQuota(
                    10737418240L, 0, 100, 3600000L,
                    107374182400L, 10000L, 1099511627776L, 1099511627776L);
            TenantUsage usage = new TenantUsage(0, 0, 0, 0);

            QuotaChecker checker = new QuotaChecker();
            QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);

            assertThat(result.passed()).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Tenant Creation/Deletion Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Tenant Lifecycle")
    class TenantLifecycle {

        @Test
        @DisplayName("Onboard creates active tenant, offboard deletes it")
        void onboardAndOffboard_fullLifecycle() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);

            // Onboard
            lifecycle.onboard("lifecycle_t1", "Lifecycle Tenant", "admin");
            Optional<Tenant> created = store.getTenant("lifecycle_t1");
            assertThat(created).isPresent();
            assertThat(created.get().status()).isEqualTo(TenantStatus.ACTIVE);
            assertThat(created.get().name()).isEqualTo("Lifecycle Tenant");

            // Offboard (suspend → delete)
            lifecycle.offboard("lifecycle_t1");
            Optional<Tenant> deleted = store.getTenant("lifecycle_t1");
            assertThat(deleted).isPresent();
            assertThat(deleted.get().status()).isEqualTo(TenantStatus.DELETED);
        }

        @Test
        @DisplayName("Suspend and reactivate tenant")
        void suspendAndReactivate() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);

            lifecycle.onboard("lifecycle_t2", "Suspend Test", "admin");
            assertThat(store.getTenant("lifecycle_t2").get().status())
                    .isEqualTo(TenantStatus.ACTIVE);

            lifecycle.suspend("lifecycle_t2");
            assertThat(store.getTenant("lifecycle_t2").get().status())
                    .isEqualTo(TenantStatus.SUSPENDED);

            lifecycle.activate("lifecycle_t2");
            assertThat(store.getTenant("lifecycle_t2").get().status())
                    .isEqualTo(TenantStatus.ACTIVE);
        }

        @Test
        @DisplayName("Deleted tenant cannot be reactivated")
        void deletedTenant_cannotBeReactivated() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);

            lifecycle.onboard("lifecycle_t3", "Delete Test", "admin");
            lifecycle.offboard("lifecycle_t3");

            assertThatThrownBy(() -> lifecycle.activate("lifecycle_t3"))
                    .hasMessageContaining("deleted");
        }

        @Test
        @DisplayName("User membership follows tenant lifecycle")
        void userMembership_followsTenantLifecycle() {
            InMemoryTenantStore store = new InMemoryTenantStore();
            TenantLifecycleManager lifecycle = new TenantLifecycleManager(store);

            lifecycle.onboard("lifecycle_t4", "Membership Test", "admin");
            store.addUserToTenant("lifecycle_t4", "alice", TenantRole.MEMBER);

            TenantResolver resolver = new TenantResolver(store);
            UserIdentity alice = new UserIdentity(
                    "alice", Set.of("team_a"), Map.of(), Instant.now());

            Optional<ResolvedTenantContext> ctx = resolver.resolve(alice);
            assertThat(ctx).isPresent();
            assertThat(ctx.get().tenant().tenantId()).isEqualTo("lifecycle_t4");

            // Remove user from tenant
            store.removeUserFromTenant("lifecycle_t4", "alice");
            Optional<ResolvedTenantContext> afterRemoval = resolver.resolve(alice);
            assertThat(afterRemoval).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Tenant createTenant(String tenantId, String name,
                                       List<String> catalogs, List<String> schemas) {
        ResourceQuota quota = new ResourceQuota(
                10737418240L, 10, 100, 3600000L,
                107374182400L, 10000L, 1099511627776L, 1099511627776L);
        return new Tenant(tenantId, name, TenantStatus.ACTIVE, quota,
                catalogs, schemas, null, Map.of(), Instant.now(), "test");
    }
}
