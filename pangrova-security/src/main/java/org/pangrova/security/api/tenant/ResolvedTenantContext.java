package org.pangrova.security.api.tenant;

/**
 * Resolved tenant membership context implementing the SPI contract.
 */
public record ResolvedTenantContext(Tenant tenant, TenantRole role)
        implements org.pangrova.security.api.TenantContext {

    @Override
    public String tenantId() {
        return tenant.tenantId();
    }

    @Override
    public String tenantName() {
        return tenant.name();
    }

    @Override
    public boolean isActive() {
        return tenant.status() == TenantStatus.ACTIVE;
    }

    @Override
    public boolean isSuspended() {
        return tenant.status() == TenantStatus.SUSPENDED;
    }

    @Override
    public String roleName() {
        return role.name();
    }
}
