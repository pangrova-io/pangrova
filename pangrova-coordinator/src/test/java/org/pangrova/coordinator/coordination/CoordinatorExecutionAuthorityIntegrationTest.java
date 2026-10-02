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
