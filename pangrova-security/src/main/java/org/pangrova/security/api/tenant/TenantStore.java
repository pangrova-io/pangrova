package org.pangrova.security.api.tenant;

import java.util.List;
import java.util.Optional;

/**
 * Persistence contract for tenant and tenant-user membership data.
 */
public interface TenantStore {

    Optional<Tenant> getTenant(String tenantId);

    List<Tenant> getAllActiveTenants();

    void createTenant(Tenant tenant);

    void updateTenant(Tenant tenant);

    Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal);

    void addUserToTenant(String tenantId, String userPrincipal, TenantRole role);

    void removeUserFromTenant(String tenantId, String userPrincipal);
}
