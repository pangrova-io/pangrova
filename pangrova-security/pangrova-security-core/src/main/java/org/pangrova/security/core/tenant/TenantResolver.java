package org.pangrova.security.core.tenant;

import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.persistence.security.api.tenant.TenantStore;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves tenant membership for a user and computes effective namespace permissions
 * as the intersection of RBAC grants and tenant-allowed namespaces.
 */
public class TenantResolver {

    private final TenantStore store;

    public TenantResolver(TenantStore store) {
        this.store = store;
    }

    public Optional<ResolvedTenantContext> resolve(UserIdentity user) {
        return store.getTenantForUser(user.principal());
    }

    public Set<String> effectiveCatalogs(ResolvedTenantContext ctx, Set<String> rbacCatalogs) {
        return intersect(ctx.tenant().allowedCatalogs(), rbacCatalogs);
    }

    public Set<String> effectiveSchemas(ResolvedTenantContext ctx, Set<String> rbacSchemas) {
        return intersect(ctx.tenant().allowedSchemas(), rbacSchemas);
    }

    private Set<String> intersect(List<String> allowed, Set<String> rbacGrants) {
        if (allowed == null || allowed.isEmpty()) {
            return rbacGrants;
        }
        return rbacGrants.stream()
                .filter(allowed::contains)
                .collect(Collectors.toSet());
    }
}
