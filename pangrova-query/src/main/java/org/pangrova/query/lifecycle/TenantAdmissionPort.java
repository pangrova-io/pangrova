package org.pangrova.query.lifecycle;

import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.tenant.ResolvedTenantContext;

import java.util.function.Function;

/** Resolves the authenticated tenant and enforces its concurrent-query quota. */
@FunctionalInterface
public interface TenantAdmissionPort {
    ResolvedTenantContext resolveTenant(UserIdentity user, Function<String, LocalUsage> tenantUsage);

    /** Coordinator-local occupancy for the resolved tenant. */
    record LocalUsage(int concurrentQueries, int queuedQueries) {}
}
