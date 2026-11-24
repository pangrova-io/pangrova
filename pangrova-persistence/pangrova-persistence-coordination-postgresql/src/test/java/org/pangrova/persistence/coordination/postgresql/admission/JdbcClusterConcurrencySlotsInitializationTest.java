package org.pangrova.persistence.coordination.postgresql.admission;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcClusterConcurrencySlotsInitializationTest extends ContainerTestBase {
    @Test
    void providerCannotCreateAnAlternativeSchemaWhenInitializationIsMissing() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        assertThatThrownBy(() -> new JdbcClusterConcurrencySlots(source))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical schema");
        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT COUNT(*) FROM information_schema.tables "
                     + "WHERE table_schema=current_schema() AND table_name LIKE 'tenant_concurrency_%'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isZero();
        }
    }

    @Test
    void canonicalInitializationAndReopeningPreserveExactLiveReservations() throws Exception {
        var source = MigratedPostgres.create();
        var first = new JdbcClusterConcurrencySlots(source);
        assertThat(first.tryReserve("tenant", "query", 1)).isTrue();
        var reopened = new JdbcClusterConcurrencySlots(source);
        assertThat(reopened.active("tenant")).isEqualTo(1);
        assertThat(reopened.tryReserve("tenant", "query", 1)).isTrue();
        assertThat(reopened.tryReserve("tenant", "another-query", 1)).isFalse();
        reopened.release("wrong-tenant", "query");
        assertThat(first.active("tenant")).isEqualTo(1);
        reopened.release("tenant", "query");
        assertThat(first.active("tenant")).isZero();
    }

    @Test
    void concurrentReservationsNearTheLimitKeepExactOwnersAndReleaseOnce() throws Exception {
        var source = MigratedPostgres.create();
        var store = new JdbcClusterConcurrencySlots(source);
        var requests = new ArrayList<Callable<Boolean>>();
        for (int sample = 0; sample < 32; sample++) {
            String id = "query-" + sample;
            requests.add(() -> store.tryReserve("tenant", id, 8));
        }
        var results = concurrently(requests);
        assertThat(results).filteredOn(Boolean::booleanValue).hasSize(8);
        assertThat(store.active("tenant")).isEqualTo(8);
        assertThat(ownerCount(source, "tenant")).isEqualTo(8);

        var releases = new ArrayList<Callable<Boolean>>();
        for (int index = 0; index < results.size(); index++) {
            if (!results.get(index)) continue;
            String id = "query-" + index;
            for (int duplicate = 0; duplicate < 2; duplicate++) {
                releases.add(() -> {
                    store.release("tenant", id);
                    return true;
                });
            }
        }
        concurrently(releases);
        assertThat(store.active("tenant")).isZero();
        assertThat(ownerCount(source, "tenant")).isZero();
    }

    @Test
    void duplicateReservationAndForeignIdentityDoNotIncreaseEitherTenantCounter() throws Exception {
        var source = MigratedPostgres.create();
        var store = new JdbcClusterConcurrencySlots(source);
        Callable<Boolean> reserve = () -> store.tryReserve("tenant", "one-query", 2);
        var results = concurrently(java.util.Collections.nCopies(12, reserve));
        assertThat(results).hasSize(12).containsOnly(true);
        assertThat(store.tryReserve("foreign", "one-query", 2)).isFalse();
        assertThat(store.active("foreign")).isZero();
        assertThat(ownerCount(source, "foreign")).isZero();
        assertThat(store.active("tenant")).isOne();
        assertThat(ownerCount(source, "tenant")).isOne();
        store.release("tenant", "one-query");
        assertThat(store.active("tenant")).isZero();
        assertThat(ownerCount(source, "tenant")).isZero();
    }

    @Test
    void counterMismatchRollsBackTheExactOwnerRemoval() throws Exception {
        var source = MigratedPostgres.create();
        var store = new JdbcClusterConcurrencySlots(source);
        assertThat(store.tryReserve("tenant", "query", 2)).isTrue();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("UPDATE tenant_concurrency_slots SET active_count = 0")).isOne();
        }
        assertThatThrownBy(() -> store.release("tenant", "query"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("ownership/count mismatch");
        assertThat(ownerCount(source, "tenant")).isOne();
    }

    private static List<Boolean> concurrently(List<Callable<Boolean>> operations) throws Exception {
        var executor = Executors.newFixedThreadPool(8);
        var start = new CountDownLatch(1);
        try {
            var results = operations.stream().map(operation -> executor.submit(() -> {
                assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                return operation.call();
            })).toList();
            start.countDown();
            var completed = new ArrayList<Boolean>(results.size());
            for (var result : results) completed.add(result.get(15, TimeUnit.SECONDS));
            return completed;
        } finally {
            executor.shutdownNow();
        }
    }

    private static int ownerCount(DataSource source, String tenant) throws Exception {
        try (var connection = source.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT COUNT(*) FROM tenant_concurrency_queries WHERE tenant_id = ?")) {
            statement.setString(1, tenant);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                return result.getInt(1);
            }
        }
    }
}
