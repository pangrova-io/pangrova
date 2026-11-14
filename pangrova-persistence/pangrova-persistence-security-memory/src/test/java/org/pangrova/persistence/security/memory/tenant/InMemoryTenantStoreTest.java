package org.pangrova.persistence.security.memory.tenant;

import org.junit.jupiter.api.Test;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTenantStoreTest {
    @Test
    void activeListingNeverIncludesSuspendedOrDeletedTenants() {
        var store = new InMemoryTenantStore();
        for (var status : TenantStatus.values()) store.createTenant(tenant(status.name(), status, null));
        assertThat(store.getAllActiveTenants()).extracting(Tenant::status).containsExactly(TenantStatus.ACTIVE);
    }

    @Test
    void staleTenantRemovalCannotEraseReassignedMembership() {
        var store = new InMemoryTenantStore();
        store.createTenant(tenant("old", TenantStatus.ACTIVE, null));
        store.createTenant(tenant("new", TenantStatus.ACTIVE, null));
        store.addUserToTenant("old", "alice", TenantRole.MEMBER);
        store.addUserToTenant("new", "alice", TenantRole.ADMIN);
        store.removeUserFromTenant("old", "alice");
        assertThat(store.getTenantForUser("alice")).hasValueSatisfying(context -> {
            assertThat(context.tenantId()).isEqualTo("new");
            assertThat(context.roleName()).isEqualTo("ADMIN");
        });
        store.removeUserFromTenant("new", "alice");
        assertThat(store.getTenantForUser("alice")).isEmpty();
    }

    @Test
    void upsertPreservesOriginalCreationMetadata() {
        var store = new InMemoryTenantStore();
        store.createTenant(tenant("one", TenantStatus.ACTIVE, null));
        var first = store.getTenant("one").orElseThrow();
        assertThat(first.createdAt()).isNotNull();
        store.updateTenant(new Tenant("one", "Updated", TenantStatus.SUSPENDED, null,
                List.of("restricted"), List.of(), null, Map.of("mode", "updated"), Instant.EPOCH, "other"));
        var updated = store.getTenant("one").orElseThrow();
        assertThat(updated.createdAt()).isEqualTo(first.createdAt());
        assertThat(updated.createdBy()).isEqualTo(first.createdBy());
        assertThat(updated.properties()).containsEntry("mode", "updated");
        assertThat(updated.status()).isEqualTo(TenantStatus.SUSPENDED);
    }

    @Test
    void absentQuotaAndExplicitZeroQuotaRemainDistinct() {
        var store = new InMemoryTenantStore();
        var zero = new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0);
        store.createTenant(tenant("absent", TenantStatus.ACTIVE, null));
        store.createTenant(tenant("zero", TenantStatus.ACTIVE, zero));
        assertThat(store.getTenant("absent").orElseThrow().quota()).isNull();
        assertThat(store.getTenant("zero").orElseThrow().quota()).isEqualTo(zero);
    }

    @Test
    void missingTenantDoesNotResolveAsAnAuthorizedMembership() {
        var store = new InMemoryTenantStore();
        store.addUserToTenant("missing", "alice", TenantRole.OWNER);
        assertThat(store.getTenantForUser("alice")).isEmpty();
    }

    private static Tenant tenant(String id, TenantStatus status, ResourceQuota quota) {
        return new Tenant(id, id, status, quota, List.of(), List.of(), null, Map.of(), null, "creator");
    }
}
