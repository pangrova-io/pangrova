package org.pangrova.persistence.security.memory.tenant;
import org.pangrova.security.api.exception.AuthorizationDeniedException;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.util.Objects;
import java.time.Instant;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.persistence.security.api.tenant.TenantStore;

/**
 * In-memory TenantStore for testing and bootstrap scenarios.
 *
 * <p>Concurrency strategy: one instance monitor protects tenant and membership snapshots.</p>
 */
@ThreadSafe
public class InMemoryTenantStore implements TenantStore {

    private final Map<String, Tenant> tenants = new HashMap<>();
    private final Map<String, UserMembership> userMemberships = new HashMap<>();

    @Override
    public synchronized Optional<Tenant> getTenant(String tenantId) {
        return Optional.ofNullable(tenants.get(Objects.requireNonNull(tenantId, "tenantId")));
    }

    @Override
    public synchronized List<Tenant> getAllActiveTenants() {
        return tenants.values().stream()
                .filter(tenant -> tenant.status() == org.pangrova.security.api.enums.TenantStatus.ACTIVE).toList();
    }

    @Override
    public synchronized void createTenant(Tenant tenant) {
        saveTenant(tenant);
    }

    @Override
    public synchronized void updateTenant(Tenant tenant) {
        saveTenant(tenant);
    }

    @Override
    public synchronized Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal) {
        UserMembership membership = userMemberships.get(Objects.requireNonNull(userPrincipal, "userPrincipal"));
        if (membership == null) {
            return Optional.empty();
        }
        return getTenant(membership.tenantId())
                .map(t -> new ResolvedTenantContext(t, membership.role()));
    }

    @Override
    public synchronized void addUserToTenant(String tenantId, String userPrincipal, TenantRole role) {
        Objects.requireNonNull(userPrincipal, "userPrincipal");
        userMemberships.put(userPrincipal, new UserMembership(
                Objects.requireNonNull(tenantId, "tenantId"), Objects.requireNonNull(role, "role")));
    }

    @Override
    public synchronized void removeUserFromTenant(String tenantId, String userPrincipal) {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(userPrincipal, "userPrincipal");
        UserMembership current = userMemberships.get(userPrincipal);
        if (current != null && current.tenantId().equals(tenantId)) {
            userMemberships.remove(userPrincipal);
        }
    }

    @Override
    public synchronized boolean mutateMembership(org.pangrova.security.api.identity.UserIdentity actor,
            org.pangrova.security.api.tenant.TenantMembershipMutation mutation) {
        Objects.requireNonNull(mutation, "mutation");
        if (actor == null || actor.principal() == null || actor.principal().isBlank()) {
            throw new AuthorizationDeniedException("Authenticated tenant administrator is required");
        }
        var current = getTenantForUser(actor.principal());
        if (current.isEmpty() || !current.orElseThrow().isActive()
                || !current.orElseThrow().tenantId().equals(mutation.tenantId())
                || current.orElseThrow().role() == TenantRole.MEMBER) {
            throw new AuthorizationDeniedException(
                    "Current OWNER or ADMIN membership in the exact active tenant is required");
        }
        var target = userMemberships.get(mutation.principal());
        if (target != null && !target.tenantId().equals(mutation.tenantId())) {
            throw new AuthorizationDeniedException("Cross-tenant membership transfer is not allowed");
        }
        if (mutation.operation() == org.pangrova.security.api.tenant.TenantMembershipMutation.Operation.GRANT) {
            if (target != null) return false;
            userMemberships.put(mutation.principal(), new UserMembership(mutation.tenantId(), TenantRole.MEMBER));
            return true;
        }
        if (target == null) return false;
        if (target.role() != TenantRole.MEMBER) {
            throw new AuthorizationDeniedException("Cannot revoke privileged tenant membership");
        }
        userMemberships.remove(mutation.principal());
        return true;
    }

    private void saveTenant(Tenant tenant) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(tenant.tenantId(), "tenantId");
        Tenant existing = tenants.get(tenant.tenantId());
        Instant createdAt = existing != null ? existing.createdAt()
                : tenant.createdAt() == null ? Instant.now() : tenant.createdAt();
        String createdBy = existing != null ? existing.createdBy() : tenant.createdBy();
        tenants.put(tenant.tenantId(), new Tenant(tenant.tenantId(), tenant.name(), tenant.status(),
                tenant.quota(), tenant.allowedCatalogs(), tenant.allowedSchemas(), tenant.resourcePool(),
                tenant.properties(), createdAt, createdBy));
    }

    private record UserMembership(String tenantId, TenantRole role) {}
}
