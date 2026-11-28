package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore.TenantCapacityExceeded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CoordinatorExecutionAuthorityIntegrationTest {
    @Test
    void losingPostgresSessionsCannotSplitTenantSlotAndAnchorCommits() throws Exception {
        var source = (org.postgresql.ds.PGSimpleDataSource)
                SharedPostgresContainer.migratedDataSource("atomic_query_slots");
        String application = "query_slot_fault_" + java.util.UUID.randomUUID();
        source.setApplicationName(application);
        var store = new JdbcExecutionAnchorStore(source);
        String id = "owned-query";
        String tenant = "one-slot-tenant";
        var lease = Duration.ofMinutes(2);

        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION block_slot_acquisition() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        PERFORM pg_advisory_xact_lock(hashtext(current_schema()), 711);
                        RETURN NEW;
                    END;
                    $$;
                    CREATE TRIGGER block_slot_acquisition BEFORE INSERT ON execution_anchor
                    FOR EACH ROW EXECUTE FUNCTION block_slot_acquisition()
                    """);
            statement.execute("""
                    CREATE FUNCTION block_slot_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        PERFORM pg_advisory_xact_lock(hashtext(current_schema()), 712);
                        RETURN OLD;
                    END;
                    $$;
                    CREATE TRIGGER block_slot_retirement BEFORE DELETE ON tenant_concurrency_queries
                    FOR EACH ROW EXECUTE FUNCTION block_slot_retirement()
                    """);
        }

        terminateBlockedStatement(source, application,
                "SELECT pg_advisory_xact_lock(hashtext(current_schema()), 711)",
                () -> store.tryAcquireLimitedQuery(id, "prior", "prior-process", lease, tenant, 1));
        assertThat(store.current(id)).isEmpty();
        assertTenantCharge(source, 0);

        var original = store.tryAcquireLimitedQuery(id, "prior", "prior-process", lease, tenant, 1)
                .orElseThrow().anchor();
        assertTenantCharge(source, 1);
        assertThatThrownBy(() -> store.tryAcquireLimitedQuery("denied", "peer", "peer-process", lease, tenant, 1))
                .isInstanceOf(TenantCapacityExceeded.class);
        assertThat(store.current("denied")).isEmpty();
        assertTenantCharge(source, 1);

        assertThat(store.yield(original)).isTrue();
        var recovered = store.claimExpired("peer", "peer-process", lease, 1).get(0);
        assertThat(store.release(original)).isFalse();
        assertTenantCharge(source, 1);
        terminateBlockedStatement(source, application,
                "SELECT pg_advisory_xact_lock(hashtext(current_schema()), 712)", () -> store.release(recovered));
        assertThat(store.current(id)).hasValueSatisfying(anchor ->
                assertThat(anchor.fencingToken()).isEqualTo(recovered.fencingToken()));
        assertTenantCharge(source, 1);

        assertThat(store.release(recovered)).isTrue();
        assertThat(store.release(recovered)).isFalse();
        assertThat(store.current(id)).isEmpty();
        assertTenantCharge(source, 0);
    }

    @Test
    void concurrentOwnersReserveExactlyTheDurableTenantCapacity() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("concurrent_owned_slots");
        var first = new JdbcExecutionAnchorStore(source);
        var second = new JdbcExecutionAnchorStore(source);
        var lease = Duration.ofMinutes(1);
        var ready = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(8);
        try {
            var admissions = new ArrayList<java.util.concurrent.Future<Optional<
                    org.pangrova.execution.api.ownership.ExecutionAnchor>>>();
            for (int index = 0; index < 24; index++) {
                var store = index % 2 == 0 ? first : second;
                String queryId = "concurrent-" + index;
                admissions.add(pool.submit(() -> {
                    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                    try {
                        return store.tryAcquireLimitedQuery(queryId, "owner-" + queryId,
                                "process", lease, "limited", 4).map(grant -> grant.anchor());
                    } catch (TenantCapacityExceeded expected) {
                        return Optional.empty();
                    }
                }));
            }
            ready.countDown();
            var acquired = new ArrayList<org.pangrova.execution.api.ownership.ExecutionAnchor>();
            for (var result : admissions) result.get(20, TimeUnit.SECONDS).ifPresent(acquired::add);
            assertThat(acquired).hasSize(4);
            assertTenantCharge(source, 4);

            var releases = acquired.stream().map(grant -> pool.submit(() -> second.release(grant))).toList();
            for (var released : releases) assertThat(released.get(10, TimeUnit.SECONDS)).isTrue();
            assertTenantCharge(source, 0);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void terminateBlockedStatement(DataSource source, String application,
            String lockSql, Runnable operation) throws Exception {
        var worker = Executors.newSingleThreadExecutor();
        try (var guard = source.getConnection(); var statement = guard.createStatement()) {
            guard.setAutoCommit(false);
            statement.execute(lockSql);
            var result = worker.submit(operation);
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                try (var connection = source.getConnection(); var find = connection.prepareStatement("""
                        SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                        WHERE application_name = ? AND wait_event = 'advisory' AND pid <> pg_backend_pid()
                        """)) {
                    find.setString(1, application);
                    try (var found = find.executeQuery()) { return found.next() && found.getBoolean(1); }
                }
            });
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class);
        } finally {
            worker.shutdownNow();
        }
    }

    private static void assertTenantCharge(DataSource source, int expected) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var counts = statement.executeQuery("""
                        SELECT COALESCE(sum(active_count), 0),
                               (SELECT COUNT(*) FROM tenant_concurrency_queries WHERE execution_bound)
                        FROM tenant_concurrency_slots
                        """)) {
            assertThat(counts.next()).isTrue();
            assertThat(counts.getInt(1)).isEqualTo(expected);
            assertThat(counts.getInt(2)).isEqualTo(expected);
        }
    }

    @Test
    void interruptedCloseRetainsRealBlockedRenewalAndRetriesOnlyAfterJdbcExit() throws Exception {
        var dataSource = (org.postgresql.ds.PGSimpleDataSource)
                SharedPostgresContainer.migratedDataSource("authority_actual_join");
        String application = "authority_join_" + java.util.UUID.randomUUID();
        dataSource.setApplicationName(application);
        var backend = new JdbcExecutionAnchorStore(dataSource);
        var authority = new CoordinatorExecutionAuthority(backend, "join-node", "join-process", Duration.ofSeconds(6));
        var anchor = authority.acquire("locked-renewal").orElseThrow();
        try (var lock = dataSource.getConnection()) {
            lock.setAutoCommit(false);
            try (var statement = lock.prepareStatement(
                    "SELECT execution_id FROM execution_anchor WHERE execution_id=? FOR UPDATE")) {
                statement.setString(1, anchor.executionId());
                try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); }
            }
            try {
                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                            "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=? "
                                    + "AND wait_event_type='Lock' "
                                    + "AND query LIKE 'SELECT execution_id FROM execution_anchor WHERE owner_id=%'")) {
                        statement.setString(1, application);
                        try (var result = statement.executeQuery()) { result.next(); return result.getInt(1) == 1; }
                    }
                });
                Thread.currentThread().interrupt();
                try {
                    assertThatThrownBy(authority::close).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("Interrupted");
                    assertThat(Thread.currentThread().isInterrupted()).isTrue();
                } finally { Thread.interrupted(); }
                assertThatThrownBy(() -> authority.acquire("late-new-job"))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
            } finally { lock.rollback(); }
            authority.close();
            authority.close();
            assertThat(backend.current(anchor.executionId())).isEmpty();
        } finally { authority.close(); }
    }

    @Test
    void maintenanceOnlyProcessRenewsTheRealBackendAndShutdownAllowsHigherTokenRecovery() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("maintenance_execution_authority");
        var backend = new JdbcExecutionAnchorStore(dataSource);
        var maintenance = new CoordinatorExecutionAuthority(backend, "maintenance-node", "first-process",
                Duration.ofSeconds(2));
        var initial = maintenance.acquireNew("artifact:maintenance-owned-job").orElseThrow();
        try {
            assertThat(maintenance.acquireNew(initial.executionId())).isEmpty();
            assertThat(maintenance.acquire(initial.executionId()).orElseThrow().fencingToken()).isEqualTo(initial.fencingToken());
            // No QUERY role or HA election service exists. The shared backend lease must still advance.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(backend.current(initial.executionId()).orElseThrow().expiresAt()).isAfter(initial.expiresAt()));
            assertThat(backend.tryAcquire(initial.executionId(), "maintenance-node", "restarted-process",
                    Duration.ofSeconds(2))).isEmpty();
        } finally {
            maintenance.close();
        }
        assertThat(backend.current(initial.executionId())).isEmpty();
        assertThatThrownBy(() -> maintenance.acquire(initial.executionId())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        try (var restarted = new CoordinatorExecutionAuthority(backend, "maintenance-node", "restarted-process",
                Duration.ofSeconds(2))) {
            var recovered = restarted.acquire(initial.executionId()).orElseThrow();
            assertThat(recovered.fencingToken()).isGreaterThan(initial.fencingToken());
            assertThat(recovered.ownerIncarnation()).isEqualTo("restarted-process");
            assertThat(backend.release(initial)).isFalse();
            assertThat(restarted.release(recovered)).isTrue();
        }
    }

    @Test
    void queryHaAndPublicationShareOneBootstrapBackendAndProcessIncarnation() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("shared_execution_binding");
        var backend = new JdbcExecutionAnchorStore(dataSource);
        try (var binding = new CoordinatorExecutionAuthority(backend, "node", "actual-process", Duration.ofSeconds(30));
                var ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(), binding.backend())) {
            ha.bindExecutionAuthority(binding);
            ha.start("node");
            assertThat(ha.acquireQueryOwnership("shared-execution", "node")).isTrue();
            var queryAnchor = ha.requireExecutionAnchor("shared-execution");
            assertThat(binding.acquire("shared-execution")).contains(queryAnchor);
            assertThat(ha.getLocalIncarnation()).isEqualTo(binding.incarnation());
            assertThat(ha.acquireQueryOwnership("shared-execution", "node")).isTrue();
            assertThat(backend.current("shared-execution").orElseThrow().fencingToken()).isEqualTo(queryAnchor.fencingToken());
        }
    }

    @Test
    void queryGrantObservesCommittedTenantPublicationRevisionWithoutChangingTheAnchor() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("publication_admission_revision");
        var owner = new JdbcExecutionAnchorStore(source);
        var peer = new JdbcExecutionAnchorStore(source);
        String tenant = "publication-" + java.util.UUID.randomUUID();
        Duration lease = Duration.ofSeconds(30);
        var first = owner.tryAcquireQuery("before-publication", "owner", "first-process", lease, tenant).orElseThrow();
        assertThat(first.publicationRevision()).hasValue(0L);
        try (var writer = source.getConnection();
                var insert = writer.prepareStatement(
                        "INSERT INTO materialization.registry_epochs(tenant_id, epoch) VALUES (?, 1)")) {
            writer.setAutoCommit(false);
            insert.setString(1, tenant);
            assertThat(insert.executeUpdate()).isOne();
            assertThat(peer.tryAcquireQuery("publication-uncommitted", "peer", "peer-process", lease, tenant)
                    .orElseThrow().publicationRevision()).hasValue(0L);
            writer.commit();
        }
        var admitted = peer.tryAcquireQuery("after-publication", "peer", "peer-process", lease, tenant).orElseThrow();
        assertThat(admitted.publicationRevision()).hasValue(1L);
        assertThat(owner.current(admitted.anchor().executionId())).contains(admitted.anchor());
        assertThat(peer.tryAcquireQuery("other-tenant-publication", "peer", "peer-process", lease, tenant + "-other")
                .orElseThrow().publicationRevision()).hasValue(0L);
        assertThat(first.publicationRevision()).hasValue(0L);
    }

    @Test
    void maintenanceAuthorityWithoutBackendFailsClosedBeforeCreatingRenewalThreads() {
        assertThatThrownBy(() -> new CoordinatorExecutionAuthority(null, "maintenance", "real-process", Duration.ofSeconds(30)))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("durable execution backend");
    }
}
