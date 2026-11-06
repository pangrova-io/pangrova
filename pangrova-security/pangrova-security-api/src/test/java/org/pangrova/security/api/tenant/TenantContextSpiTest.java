package org.pangrova.security.api.tenant;

import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantRole;
import org.pangrova.security.api.tenant.TenantStatus;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that TenantContext record correctly implements the SPI contract
 * defined in pangrova-security (org.pangrova.security.api.tenant.TenantContext).
 */
class TenantContextSpiTest {

    private static final ResourceQuota DEFAULT_QUOTA = new ResourceQuota(
            10737418240L, 10, 100, 3600000L,
            107374182400L, 10000L, 1099511627776L, 1099511627776L);

    @Test
    void implementsSpiInterface() {
        ResolvedTenantContext ctx = createContext(TenantStatus.ACTIVE, TenantRole.ADMIN);
        // Verify the record implements the SPI interface from pangrova-security
        assertThat(ctx).isInstanceOf(org.pangrova.security.api.tenant.TenantContext.class);
    }

    @Test
    void activeTenantReportsCorrectState() {
        ResolvedTenantContext ctx = createContext(TenantStatus.ACTIVE, TenantRole.MEMBER);

        assertThat(ctx.tenantId()).isEqualTo("t-001");
        assertThat(ctx.tenantName()).isEqualTo("Engineering");
        assertThat(ctx.isActive()).isTrue();
        assertThat(ctx.isSuspended()).isFalse();
        assertThat(ctx.roleName()).isEqualTo("MEMBER");
    }

    @Test
    void suspendedTenantReportsCorrectState() {
        ResolvedTenantContext ctx = createContext(TenantStatus.SUSPENDED, TenantRole.OWNER);

        assertThat(ctx.isActive()).isFalse();
        assertThat(ctx.isSuspended()).isTrue();
        assertThat(ctx.roleName()).isEqualTo("OWNER");
    }

    @Test
    void deletedTenantIsNeitherActiveNorSuspended() {
        ResolvedTenantContext ctx = createContext(TenantStatus.DELETED, TenantRole.ADMIN);

        assertThat(ctx.isActive()).isFalse();
        assertThat(ctx.isSuspended()).isFalse();
    }

    private ResolvedTenantContext createContext(TenantStatus status, TenantRole role) {
        Tenant tenant = new Tenant("t-001", "Engineering", status, DEFAULT_QUOTA,
                List.of("iceberg"), List.of("default"), "pool-1",
                Map.of(), Instant.now(), "admin");
        return new ResolvedTenantContext(tenant, role);
    }
}
