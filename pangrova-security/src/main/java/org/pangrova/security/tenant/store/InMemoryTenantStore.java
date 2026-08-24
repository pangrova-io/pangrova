package org.pangrova.security.tenant.store;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory TenantStore for testing and bootstrap scenarios.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for safe concurrent access.</p>
 */
@ThreadSafe
public class InMemoryTenantStore implements TenantStore {

    private final Map<String, Tenant> tenants = new ConcurrentHashMap<>();
    private final Map<String, UserMembership> userMemberships = new ConcurrentHashMap<>();

    @Override
    public Optional<Tenant> getTenant(String tenantId) {
        return Optional.ofNullable(tenants.get(tenantId));
    }

    @Override
    public List<Tenant> getAllActiveTenants() {
        return List.copyOf(tenants.values());
    }

    @Override
    public void createTenant(Tenant tenant) {
        tenants.put(tenant.tenantId(), tenant);
    }

    @Override
    public void updateTenant(Tenant tenant) {
        tenants.put(tenant.tenantId(), tenant);
    }

    @Override
    public Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal) {
        UserMembership membership = userMemberships.get(userPrincipal);
        if (membership == null) {
            return Optional.empty();
        }
        return getTenant(membership.tenantId())
                .map(t -> new ResolvedTenantContext(t, membership.role()));
    }

    @Override
    public void addUserToTenant(String tenantId, String userPrincipal, TenantRole role) {
        userMemberships.put(userPrincipal, new UserMembership(tenantId, role));
    }

    @Override
    public void removeUserFromTenant(String tenantId, String userPrincipal) {
        userMemberships.remove(userPrincipal);
    }

    private record UserMembership(String tenantId, TenantRole role) {}
}
