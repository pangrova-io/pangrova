package org.pangrova.persistence.api.security.tenant;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.spi.security.tenant.ResolvedTenantContext;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;

import java.util.List;
import java.util.Optional;

/**
 * Persistence contract for tenant and tenant-user membership data.
 */
@Experimental
public interface TenantStore {

    Optional<Tenant> getTenant(String tenantId);

    List<Tenant> getAllActiveTenants();

    void createTenant(Tenant tenant);

    void updateTenant(Tenant tenant);

    Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal);

    void addUserToTenant(String tenantId, String userPrincipal, TenantRole role);

    void removeUserFromTenant(String tenantId, String userPrincipal);
}
