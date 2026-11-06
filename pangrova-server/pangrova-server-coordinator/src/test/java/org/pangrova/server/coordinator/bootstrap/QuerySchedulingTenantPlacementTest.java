package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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

    private static Tenant tenant(TenantStatus status, String pool) {
        return new Tenant("tenant-a", "Tenant A", status, new ResourceQuota(1024, 1, 1, 1000, 1024, 100, 1024, 1024),
                List.of(), List.of(), pool, Map.of(), Instant.EPOCH, "test");
    }
}
