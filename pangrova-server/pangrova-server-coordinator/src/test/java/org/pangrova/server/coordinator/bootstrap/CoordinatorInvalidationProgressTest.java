package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.server.coordinator.enums.CoordinatorRole;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordination.api.leadership.MaintenanceLeaderAware;
import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.persistence.coordination.postgresql.lease.JdbcDistributedLease;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcCoordinatorRegistry;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.pangrova.persistence.coordination.memory.membership.InMemoryCoordinatorRegistry;

/**
 * A dead Coordinator must not pin the invalidation-log cleanup floor. Asking the
 * registry for "every Coordinator ever registered" computed a cutoff far in the
 * past, so a long-dead Coordinator's watermark (often 0) blocked purging forever.
 */
class CoordinatorInvalidationProgressTest {

    private static CoordinatorSnapshot coordinator(
            String id, long lastHeartbeatMs, long lastProcessedInvalidationId) {
        return new CoordinatorSnapshot(
                id, "host-" + id, 8080, "COMBINED", 1L,
                lastHeartbeatMs, lastHeartbeatMs - 1000, lastProcessedInvalidationId);
    }

    @Test
    void deadCoordinatorDoesNotBoundTheFloor() {
        var registry = new InMemoryCoordinatorRegistry();
        long now = System.currentTimeMillis();
        registry.register(coordinator("live", now, 500));
        // Heartbeat far older than the 5 minute liveness window.
        registry.register(coordinator("dead", now - Duration.ofHours(2).toMillis(), 0));

        var adapter = new CoordinatorInvalidationProgress(
                registry, CoordinatorInvalidationProgress.DEFAULT_LIVENESS_WINDOW);

        assertThat(adapter.minimumReportedInvalidationId()).isEqualTo(500);
    }

    @Test
    void slowestLiveCoordinatorBoundsTheFloor() {
        var registry = new InMemoryCoordinatorRegistry();
        long now = System.currentTimeMillis();
        registry.register(coordinator("fast", now, 900));
        registry.register(coordinator("slow", now, 400));

        var adapter = new CoordinatorInvalidationProgress(
                registry, CoordinatorInvalidationProgress.DEFAULT_LIVENESS_WINDOW);

        assertThat(adapter.minimumReportedInvalidationId()).isEqualTo(400);
    }

    @Test
    void noLiveCoordinatorLeavesTheFloorUnbounded() {
        var registry = new InMemoryCoordinatorRegistry();
        // Nothing bounds the floor, which the poller treats as "no constraint".
        assertThat(new CoordinatorInvalidationProgress(
                registry, CoordinatorInvalidationProgress.DEFAULT_LIVENESS_WINDOW)
                .minimumReportedInvalidationId())
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void reportsThisCoordinatorsOwnWatermarkForRestartResume() {
        var registry = new InMemoryCoordinatorRegistry();
        long now = System.currentTimeMillis();
        registry.register(coordinator("self", now, 700));

        var adapter = new CoordinatorInvalidationProgress(
                registry, CoordinatorInvalidationProgress.DEFAULT_LIVENESS_WINDOW);

        assertThat(adapter.lastProcessedInvalidationId("self")).isEqualTo(700);
        assertThat(adapter.lastProcessedInvalidationId("unknown")).isEqualTo(-1L);
    }

    @Test
    void nonPositiveLivenessWindowIsRejected() {
        var registry = new InMemoryCoordinatorRegistry();
        assertThatThrownBy(() -> new CoordinatorInvalidationProgress(registry, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("livenessWindow");
        assertThatThrownBy(() -> new CoordinatorInvalidationProgress(registry, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
    @Test
    void queryRegistrationResumesBeforeApplyingOnlyNewerCommittedEntries() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("query_invalidation_restart");
        insertAgedEntries(source);
        var registry = new JdbcCoordinatorRegistry(source);
        var registration = coordinator("query", System.currentTimeMillis(), -1);
        registry.register(registration);
        registry.updateLastProcessedInvalidationId("query", 1);
        registry.register(registration);
        var applied = new CopyOnWriteArrayList<String>();
        var cache = new TieredMetadataCache() {
            @Override public void invalidateCatalog(String catalog) {
                applied.add(catalog);
                super.invalidateCatalog(catalog);
            }
        };
        try (var reader = CoordinatorInvalidationProgress.createPoller(
                source, cache, registry, CoordinatorRole.QUERY, "query")) {
            reader.start();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(registry.getCoordinator("query").orElseThrow().lastProcessedInvalidationId()).isEqualTo(2));
            assertThat(applied).containsExactly("second");
        }
    }

    @Test
    void maintenanceReacquiresLeaseAndRereadsTheSlowestLiveQueryWatermark() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("maintenance_invalidation_floor");
        insertAgedEntries(source);
        var registry = new JdbcCoordinatorRegistry(source);
        registry.register(coordinator("query", System.currentTimeMillis(), -1));
        var lease = new JdbcDistributedLease(source);
        try (var poller = CoordinatorInvalidationProgress.createPoller(
                source, new TieredMetadataCache(), registry, CoordinatorRole.MAINTENANCE, "maintenance")) {
            poller.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> poller.getLastProcessedId() == 2);
            assertThat(registry.getCoordinator("maintenance")).isEmpty();
            var removed = new CopyOnWriteArrayList<Integer>();
            var cleanup = new MaintenanceLeaderAware() {
                @Override public void onLeaderElected() { removed.add(poller.cleanup()); }
                @Override public void onLeadershipLost() {}
            };
            var local = new MaintenanceLeaderElection(lease, "maintenance");
            var peer = new MaintenanceLeaderElection(lease, "other");
            try (var leadership = new MaintenanceLeadershipBootstrap(local, cleanup)) {
                leadership.poll();
                long initialEpoch = leadership.activeEpoch();
                assertThat(removed).containsExactly(0);
                assertThat(local.release()).isTrue();
                assertThat(peer.tryAcquireOrRenew()).isTrue();
                leadership.poll();
                assertThat(leadership.isLeader()).isFalse();
                registry.updateLastProcessedInvalidationId("query", 1);
                assertThat(peer.release()).isTrue();
                leadership.poll();
                assertThat(leadership.activeEpoch()).isGreaterThan(initialEpoch);
                assertThat(removed).containsExactly(0, 1);
                registry.updateLastProcessedInvalidationId("query", 2);
                assertThat(poller.cleanup()).isEqualTo(1);
            } finally {
                peer.release();
            }
        }
    }

    private static void insertAgedEntries(DataSource source) throws Exception {
        try (var conn = source.getConnection(); var stmt = conn.prepareStatement(
                "INSERT INTO invalidation_log(cache_key, invalidation_type, created_at, updated_at)"
                        + " VALUES (?, 'CATALOG', NOW() - INTERVAL '2 days', NOW())")) {
            for (String key : List.of("first", "second")) {
                stmt.setString(1, key);
                stmt.executeUpdate();
            }
        }
    }

}