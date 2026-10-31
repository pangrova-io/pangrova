package org.pangrova.scheduler.core.admission;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.testing.testkit.postgres.SharedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.scheduler.store.jdbc.admission.JdbcClusterConcurrencySlots;

/**
 * A-ADMISSION-01: verifies the atomic cluster-wide concurrency gate never oversubscribes a tenant's
 * limit, including under concurrent reservation from multiple "coordinators" sharing one store.
 */
class ClusterConcurrencySlotsTest extends ContainerTestBase {

    private static DataSource postgres(String name) {
        try {
            return SharedPostgres.isolatedDataSource();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create isolated test schema", e);
        }
    }

    @Test
    void jdbcReserveRespectsLimit() {
        ClusterConcurrencySlots slots = new JdbcClusterConcurrencySlots(
                postgres("slots_limit"));
        assertThat(slots.tryReserve("t", "q1", 2)).isTrue();
        assertThat(slots.tryReserve("t", "q2", 2)).isTrue();
        assertThat(slots.tryReserve("t", "q3", 2)).as("3rd exceeds limit 2").isFalse();
        assertThat(slots.active("t")).isEqualTo(2);
    }

    @Test
    void jdbcReleaseFreesSlot() {
        ClusterConcurrencySlots slots = new JdbcClusterConcurrencySlots(
                postgres("slots_release"));
        assertThat(slots.tryReserve("t", "q1", 1)).isTrue();
        assertThat(slots.tryReserve("t", "q2", 1)).as("at limit").isFalse();
        slots.release("t", "q1");
        assertThat(slots.active("t")).isZero();
        assertThat(slots.tryReserve("t", "q2", 1)).as("slot freed after release").isTrue();
    }

    @Test
    void jdbcConcurrentReservesFromTwoCoordinatorsNeverOversell() throws InterruptedException {
        // Two coordinators sharing ONE store burst-admit; the atomic gate must cap total at the limit.
        DataSource shared = postgres("slots_concurrent");
        ClusterConcurrencySlots coordA = new JdbcClusterConcurrencySlots(shared);
        ClusterConcurrencySlots coordB = new JdbcClusterConcurrencySlots(shared);
        int limit = 5;
        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger reserved = new AtomicInteger();
        try {
            for (int i = 0; i < threads; i++) {
                ClusterConcurrencySlots coord = (i % 2 == 0) ? coordA : coordB;
                String queryId = "q" + i;
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (coord.tryReserve("t", queryId, limit)) {
                        reserved.incrementAndGet();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(reserved.get()).as("no cross-coordinator oversell").isEqualTo(limit);
        assertThat(coordA.active("t")).as("store reflects exactly the limit").isEqualTo(limit);
    }

    @Test
    void inMemoryReserveRespectsLimitAndRelease() {
        ClusterConcurrencySlots slots = new InMemoryClusterConcurrencySlots();
        assertThat(slots.tryReserve("t", "q1", 1)).isTrue();
        assertThat(slots.tryReserve("t", "q2", 1)).isFalse();
        slots.release("t", "q1");
        assertThat(slots.tryReserve("t", "q2", 1)).isTrue();
        assertThat(slots.active("t")).isEqualTo(1);
    }

    @Test
    void inMemoryConcurrentReservesNeverOversell() throws InterruptedException {
        ClusterConcurrencySlots slots = new InMemoryClusterConcurrencySlots();
        int limit = 3;
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger reserved = new AtomicInteger();
        try {
            for (int i = 0; i < threads; i++) {
                String queryId = "q" + i;
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (slots.tryReserve("t", queryId, limit)) {
                        reserved.incrementAndGet();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(reserved.get()).isEqualTo(limit);
    }

    @Test
    void restartedJdbcOwnerCanReleaseOnlyTheExactReservation() {
        var source = postgres("restarted_owner");
        var original = new JdbcClusterConcurrencySlots(source);
        assertThat(original.tryReserve("t", "q1", 2)).isTrue();
        assertThat(original.tryReserve("t", "q2", 2)).isTrue();
        var restarted = new JdbcClusterConcurrencySlots(source);

        assertThat(restarted.tryReserve("t", "q1", 2)).isTrue();
        restarted.release("t", "unknown");
        restarted.release("another-tenant", "q1");
        assertThat(restarted.active("t")).isEqualTo(2);
        restarted.release("t", "q1");
        restarted.release("t", "q1");
        assertThat(original.active("t")).isEqualTo(1);
        assertThat(original.tryReserve("t", "q3", 2)).isTrue();
        assertThat(original.tryReserve("t", "q4", 2)).isFalse();
    }

    @Test
    void queryIdentityCannotBeReservedForTwoTenants() {
        for (var slots : java.util.List.of(new InMemoryClusterConcurrencySlots(),
                new JdbcClusterConcurrencySlots(postgres("identity_mismatch")))) {
            assertThat(slots.tryReserve("first", "query", 2)).isTrue();
            assertThat(slots.tryReserve("second", "query", 2)).isFalse();
            slots.release("second", "query");
            assertThat(slots.active("first")).isEqualTo(1);
            assertThat(slots.active("second")).isZero();
        }
    }

    @Test
    void backendFailureIsNotZeroUsageOrSuccessfulRelease() {
        var source = (PGSimpleDataSource) postgres("backend_auth_failure");
        var slots = new JdbcClusterConcurrencySlots(source);
        assertThat(slots.tryReserve("t", "query", 1)).isTrue();
        var password = source.getPassword();
        source.setPassword("invalid-password");
        try {
            assertThatThrownBy(() -> slots.active("t")).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> slots.release("t", "query")).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> slots.tryReserve("t", "another-query", 1))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            source.setPassword(password);
        }
        assertThat(slots.active("t")).isEqualTo(1);
        slots.release("t", "query");
        assertThat(slots.active("t")).isZero();
    }

    @Test
    void controllerKeepsTheExactOwnerWhenBackendReleaseFails() {
        var source = (PGSimpleDataSource) postgres("controller_release_retry");
        var slots = new JdbcClusterConcurrencySlots(source);
        var controller = controller(slots);
        assertThat(controller.admit(org.pangrova.scheduler.api.resource.AdmissionRequest
                .of("t", "default", "query")).isAdmitted()).isTrue();
        var password = source.getPassword();
        source.setPassword("invalid-password");
        try {
            assertThatThrownBy(() -> controller.release("query", "t")).isInstanceOf(IllegalStateException.class);
        } finally {
            source.setPassword(password);
        }
        assertThat(controller.admittedCount("t")).isEqualTo(1);
        controller.release("unknown-query", "t");
        assertThat(controller.admittedCount("t")).isEqualTo(1);
        controller.release("query", "t");
        controller.release("query", "t");
        assertThat(controller.admittedCount("t")).isZero();
    }

    @Test
    void replacementControllerCanReturnOnlyThePersistedQueryReservation() {
        var source = postgres("controller_replacement");
        var original = controller(new JdbcClusterConcurrencySlots(source));
        assertThat(original.admit(org.pangrova.scheduler.api.resource.AdmissionRequest
                .of("t", "default", "first")).isAdmitted()).isTrue();
        assertThat(original.admit(org.pangrova.scheduler.api.resource.AdmissionRequest
                .of("t", "default", "second")).isAdmitted()).isTrue();
        var replacement = controller(new JdbcClusterConcurrencySlots(source));

        replacement.release("first", "wrong-tenant");
        replacement.release("unknown-query", "t");
        assertThat(replacement.admittedCount("t")).isEqualTo(2);
        replacement.release("first", "t");
        replacement.release("first", "t");
        assertThat(original.admittedCount("t")).isEqualTo(1);
        original.release("first", "t");
        assertThat(replacement.admittedCount("t")).isEqualTo(1);
        replacement.release("second", "t");
        assertThat(original.admittedCount("t")).isZero();
    }

    @Test
    void legacyAnonymousReservationsCannotBeRelabeledOrReset() throws SQLException {
        var source = postgres("unidentified_legacy_count");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE tenant_concurrency_slots "
                    + "(tenant_id VARCHAR(255) PRIMARY KEY, active_count INT NOT NULL)");
            statement.execute("INSERT INTO tenant_concurrency_slots VALUES ('t', 2)");
        }

        assertThatThrownBy(() -> new JdbcClusterConcurrencySlots(source))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("unidentified reservation");

        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var count = statement.executeQuery("SELECT active_count FROM tenant_concurrency_slots WHERE tenant_id='t'")) {
            assertThat(count.next()).isTrue();
            assertThat(count.getInt(1)).isEqualTo(2);
        }
    }

    private static TenantQuotaAdmissionController controller(ClusterConcurrencySlots slots) {
        var tenants = new org.pangrova.security.core.tenant.InMemoryTenantStore();
        var quota = new org.pangrova.security.api.tenant.ResourceQuota(1000, 2, 1, 100, 100, 100, 100, 100);
        tenants.createTenant(new org.pangrova.security.api.tenant.Tenant("t", "Tenant",
                org.pangrova.security.api.tenant.TenantStatus.ACTIVE, quota, java.util.List.of(),
                java.util.List.of(), null, java.util.Map.of(), java.time.Instant.EPOCH, "admin"));
        return new TenantQuotaAdmissionController(tenants, null, slots);
    }
}
