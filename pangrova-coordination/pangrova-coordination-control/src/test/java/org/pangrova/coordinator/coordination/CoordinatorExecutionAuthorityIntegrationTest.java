package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CoordinatorExecutionAuthorityIntegrationTest {
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
        var initial = maintenance.acquire("artifact:maintenance-owned-job").orElseThrow();
        try {
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
