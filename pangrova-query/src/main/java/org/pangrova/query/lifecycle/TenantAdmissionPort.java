package org.pangrova.query.lifecycle;

import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.tenant.ResolvedTenantContext;

import java.util.Collection;

/** Resolves the authenticated tenant and enforces its concurrent-query quota. */
@FunctionalInterface
public interface TenantAdmissionPort {
    ResolvedTenantContext resolveTenant(UserIdentity user, Collection<QueryStateMachine> activeQueries);
}
