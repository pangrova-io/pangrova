package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuerySchedulingTenantPlacementTest {
    @Test
    void configuredAuthorityDistinguishesSharedPoolUnknownAndInactiveTenants() {
        var store = new InMemoryTenantStore();
        store.createTenant(tenant(TenantStatus.ACTIVE, null));
        var resolver = QuerySchedulingWiring.tenantPlacement(store);
        assertThat(resolver.resourcePool("tenant-a")).isEmpty();
        store.updateTenant(tenant(TenantStatus.ACTIVE, "pool-b"));
        assertThat(resolver.resourcePool("tenant-a")).contains("pool-b");
        store.updateTenant(tenant(TenantStatus.SUSPENDED, "pool-b"));
        assertThatThrownBy(() -> resolver.resourcePool("tenant-a"))
                .isInstanceOf(NoAvailableWorkerException.class).hasMessageContaining("Inactive");
        assertThatThrownBy(() -> resolver.resourcePool("missing"))
                .isInstanceOf(NoAvailableWorkerException.class).hasMessageContaining("Unknown");
        assertThatThrownBy(() -> resolver.resourcePool(null)).isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void deploymentWithoutTenantAuthorityExplicitlyUsesSharedWorkers() {
        assertThat(QuerySchedulingWiring.tenantPlacement(null).resourcePool("identity-not-pool")).isEmpty();
    }

    @Test
    @Tag("integration")
    void stageCohortReadsTheJdbcTenantOnceAndNextStageObservesPoolChanges() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("stage_placement");
        var writer = new JdbcTenantStore(source);
        writer.createTenant(tenant(TenantStatus.ACTIVE, "pool-a"));
        var reads = new AtomicInteger();
        var counted = new PGSimpleDataSource() {
            @Override public Connection getConnection() throws SQLException {
                reads.incrementAndGet();
                return super.getConnection();
            }
        };
        var postgres = SharedPostgresContainer.instance();
        counted.setURL(postgres.getJdbcUrl());
        counted.setUser(postgres.getUsername());
        counted.setPassword(postgres.getPassword());
        try (var connection = source.getConnection()) {
            counted.setCurrentSchema(connection.getSchema());
        }
        var scheduler = new TaskScheduler(new LoadBalancedStrategy(),
                QuerySchedulingWiring.tenantPlacement(new JdbcTenantStore(counted)));
        var original = worker("pool-a");
        var next = worker("pool-b");
        var candidates = List.of(original, next);
        for (int partition = 0; partition < 64; partition++) {
            assertThat(scheduler.selectWorkerForTenant(1L, candidates, "tenant-a")).contains(original);
        }
        assertThat(reads).hasValue(64);

        reads.set(0);
        var cohort = scheduler.bindTenant("tenant-a");
        writer.updateTenant(tenant(TenantStatus.ACTIVE, "pool-b"));
        for (int partition = 0; partition < 64; partition++) {
            assertThat(cohort.selectWorkerForTenant(1L, candidates, "tenant-a")).contains(original);
        }
        assertThatThrownBy(() -> cohort.selectWorkerForTenant(1L, candidates, "another"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(reads).hasValue(1);
        assertThat(scheduler.bindTenant("tenant-a").selectWorkerForTenant(1L, candidates, "tenant-a")).contains(next);
        writer.updateTenant(tenant(TenantStatus.SUSPENDED, "pool-b"));
        assertThatThrownBy(() -> scheduler.bindTenant("tenant-a")).isInstanceOf(NoAvailableWorkerException.class);
        assertThat(reads).hasValue(3);
    }

    private static WorkerInfo worker(String pool) {
        var worker = TestWorkerInfoBuilder.active(pool, "127.0.0.1", 8080, 8081);
        return new WorkerInfo(worker.workerId(), worker.host(), worker.httpPort(), worker.transportPort(),
                worker.resultServerPort(), worker.healthState(), worker.memoryPressure(),
                worker.cpuUtilization(), worker.runningTaskCount(), worker.availableMemoryBytes(),
                worker.lastHeartbeat(), worker.performanceScore(), pool, worker.protocolVersion(), worker.rackId(),
                worker.dataLocality(), worker.cacheSummary(), worker.capabilityInventory(), worker.copyTaskTlsPort(),
                worker.workerExecutionIdentity(), worker.shuffleDeployment());
    }

    private static Tenant tenant(TenantStatus status, String pool) {
        return new Tenant("tenant-a", "Tenant A", status, new ResourceQuota(1024, 1, 1, 1000, 1024, 100, 1024, 1024),
                List.of(), List.of(), pool, Map.of(), Instant.EPOCH, "test");
    }
}
