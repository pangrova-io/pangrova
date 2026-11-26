package org.pangrova.coordinator.query.tenant;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.tenant.QuotaChecker;
import org.pangrova.security.exception.TenantQuotaExceededException;
import org.pangrova.security.tenant.TenantUsage;
import org.pangrova.security.tenant.TenantResolver;
import org.pangrova.security.tenant.TenantQueryGuard;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for TenantQueryGuard fail-close semantics.
 * Validates that unmapped tenants are denied (Requirement 11.4).
 */
class TenantQueryGuardTest {

    private InMemoryTenantStore store;
    private TenantResolver resolver;
    private QuotaChecker quotaChecker;
    private TenantQueryGuard guard;

    @BeforeEach
    void setUp() {
        store = new InMemoryTenantStore();
        resolver = new TenantResolver(store);
        quotaChecker = new QuotaChecker();
        guard = new TenantQueryGuard(resolver, quotaChecker, this::zeroUsage);
    }

    @Test
    void mappedActiveTenantReturnsId() {
        store.createTenant(activeTenant("t-active", 10));
        store.addUserToTenant("t-active", "alice", TenantRole.MEMBER);

        var result = guard.checkAndResolve(identity("alice"));

        assertThat(result.tenantId()).isEqualTo("t-active");
        assertThat(result.tenant().quota().maxConcurrentQueries()).isEqualTo(10);
    }

    @Test
    void unmappedUserDeniedWithFailClose() {
        UserIdentity unknown = identity("nobody");

        assertThatThrownBy(() -> guard.checkAndResolve(unknown))
                .isInstanceOf(AuthorizationDeniedException.class)
                .satisfies(ex -> {
                    PangrovaException ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED);
                })
                .hasMessageContaining("no tenant mapping")
                .hasMessageContaining("fail-close")
                .hasMessageContaining("nobody");
    }

    @Test
    void nullIdentityDeniedWithFailClose() {
        assertThatThrownBy(() -> guard.checkAndResolve(null))
                .isInstanceOf(AuthorizationDeniedException.class)
                .satisfies(ex -> {
                    PangrovaException ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED);
                })
                .hasMessageContaining("no user identity")
                .hasMessageContaining("fail-close");
    }

    @Test
    void suspendedTenantDenied() {
        store.createTenant(suspendedTenant("t-suspended"));
        store.addUserToTenant("t-suspended", "bob", TenantRole.MEMBER);

        assertThatThrownBy(() -> guard.checkAndResolve(identity("bob")))
                .isInstanceOf(TenantQuotaExceededException.class)
                .hasMessageContaining("SUSPENDED");
    }

    @Test
    void deletedTenantDenied() {
        var tenant = activeTenant("t-deleted", 10);
        store.createTenant(new Tenant(tenant.tenantId(), tenant.name(), TenantStatus.DELETED, tenant.quota(),
                tenant.allowedCatalogs(), tenant.allowedSchemas(), tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy()));
        store.addUserToTenant("t-deleted", "bob", TenantRole.MEMBER);

        assertThatThrownBy(() -> guard.checkAndResolve(identity("bob")))
                .isInstanceOf(TenantQuotaExceededException.class)
                .hasMessageContaining("DELETED");
    }

    @Test
    void quotaExceededDenied() {
        store.createTenant(activeTenant("t-quota", 0));
        store.addUserToTenant("t-quota", "charlie", TenantRole.MEMBER);

        TenantQueryGuard guardWithUsage = new TenantQueryGuard(
                resolver, quotaChecker, tenantId -> new TenantUsage(1, 0, 0L, 0L));

        assertThatThrownBy(() -> guardWithUsage.checkAndResolve(identity("charlie")))
                .isInstanceOf(TenantQuotaExceededException.class)
                .hasMessageContaining("quota exceeded");
    }

    private TenantUsage zeroUsage(String tenantId) {
        return new TenantUsage(0, 0, 0L, 0L);
    }

    private static UserIdentity identity(String principal) {
        return new UserIdentity(principal, Set.of(), Map.of(), Instant.now());
    }

    private static Tenant activeTenant(String tenantId, int maxConcurrent) {
        ResourceQuota quota = new ResourceQuota(
                1024L * 1024 * 1024, maxConcurrent, 100, 60_000L,
                1024L * 1024 * 1024, 10_000L,
                1024L * 1024 * 1024 * 1024L, 1024L * 1024 * 1024 * 1024L);
        return new Tenant(tenantId, tenantId + "-name", TenantStatus.ACTIVE, quota,
                List.of(), List.of(), null, Map.of(), Instant.now(), "system");
    }

    private static Tenant suspendedTenant(String tenantId) {
        ResourceQuota quota = new ResourceQuota(
                1024L * 1024 * 1024, 10, 100, 60_000L,
                1024L * 1024 * 1024, 10_000L,
                1024L * 1024 * 1024 * 1024L, 1024L * 1024 * 1024 * 1024L);
        return new Tenant(tenantId, tenantId + "-name", TenantStatus.SUSPENDED, quota,
                List.of(), List.of(), null, Map.of(), Instant.now(), "system");
    }
}
