package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CoordinatorExecutionAuthorityIntegrationTest {
    @Test
    void unifiedRenewalRefreshesTheActualQueryPublicationDeadline() throws Exception {
        var backend = new JdbcExecutionAnchorStore(
                SharedPostgresContainer.migratedDataSource("query_renewal_receipt"));
        try (var authority = new CoordinatorExecutionAuthority(backend, "node", "process", Duration.ofSeconds(2));
                var ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease())) {
            ha.bindExecutionAuthority(authority);
            ha.start("node");
            assertThat(ha.acquireQueryOwnership("query", "node")).isTrue();
            var initial = ha.requireExecutionAnchor("query");
            assertThat(Duration.between(initial.acquiredAt(), initial.expiresAt())).isEqualTo(Duration.ofSeconds(2));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                var renewed = ha.requireExecutionAnchor("query");
                assertThat(renewed.expiresAt()).isAfter(initial.expiresAt());
                assertThat(renewed.fencingToken()).isEqualTo(initial.fencingToken());
                assertThat(renewed.acquiredAt()).isEqualTo(initial.acquiredAt());
                assertThat(ha.validateDurableExecutionToken("query", renewed.fencingToken())).isTrue();
            });
            assertThat(ha.acquireQueryOwnership("foreign", "another-node")).isFalse();
            assertThatThrownBy(() -> ha.setExecutionAnchorStore(backend))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot change");
        }
    }

    @Test
    void unifiedRenewalInvalidatesARealLostQueryDespiteLiveMaintenanceAnchors() throws Exception {
        var backend = new JdbcExecutionAnchorStore(
                SharedPostgresContainer.migratedDataSource("query_renewal_lost_fence"));
        try (var authority = new CoordinatorExecutionAuthority(backend, "node", "process", Duration.ofSeconds(2));
                var ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease())) {
            ha.bindExecutionAuthority(authority);
            ha.start("node");
            assertThat(ha.acquireQueryOwnership("lost", "node")).isTrue();
            var lost = ha.requireExecutionAnchor("lost");
            assertThat(backend.release(lost)).isTrue();
            var maintenance = authority.acquire("maintenance").orElseThrow();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(ha.validateExecutionToken("lost", lost.fencingToken())).isFalse();
                assertThat(backend.current("maintenance").orElseThrow().expiresAt())
                        .isAfter(maintenance.expiresAt());
            });
            assertThatThrownBy(() -> ha.requireExecutionAnchor("lost"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("No live local");
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
                                    + "AND wait_event_type='Lock' AND query LIKE '%UPDATE execution_anchor%SET expires_at=%'")) {
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
                var ha = new HighAvailabilityCoordinationService(new InMemoryDistributedLease())) {
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
    void maintenanceAuthorityWithoutBackendFailsClosedBeforeCreatingRenewalThreads() {
        assertThatThrownBy(() -> new CoordinatorExecutionAuthority(null, "maintenance", "real-process", Duration.ofSeconds(30)))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("durable execution backend");
    }
}
