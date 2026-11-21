package org.pangrova.persistence.security.api.tenant;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;

import java.util.List;
import java.util.Optional;

/**
 * Persistence contract for tenant and tenant-user membership data.
 * Implementations are thread-safe. Tenant writes atomically store definition, quota,
 * namespace restrictions and properties. An absent quota and an explicit all-zero quota
 * are distinct values. Memory implementations do not promise restart durability.
 * Membership resolves with the matching tenant in one backend snapshot; a missing
 * tenant resolves as absent. Authorization callers must still reject inactive tenants.
 */
@Experimental
public interface TenantStore {

    /**
     * Atomically authorizes the actor's current OWNER/ADMIN membership in the exact ACTIVE
     * tenant and grants/revokes MEMBER access. Never changes namespace restrictions or
     * transfers a principal from another tenant. GRANT preserves existing privileged
     * roles; REVOKE refuses OWNER/ADMIN removal. Returns true only if membership changed.
     * Replays recheck current authority; authorized identical operations are idempotent.
     * PostgreSQL serializes with tenant/authority mutations; memory is instance-local.
     * A lost commit acknowledgement throws TenantMembershipOutcomeUnknownException.
     */
    boolean mutateMembership(org.pangrova.security.api.identity.UserIdentity actor,
            org.pangrova.security.api.tenant.TenantMembershipMutation mutation);

    Optional<Tenant> getTenant(String tenantId);

    /** Returns only ACTIVE tenants; ordering is unspecified. */
    List<Tenant> getAllActiveTenants();

    /**
     * Atomically upserts the tenant, preserving original creation time/creator if it exists.
     * A missing creation time is generated on first insertion. Backend failures propagate;
     * an exception during commit does not prove the write was rejected.
     */
    void createTenant(Tenant tenant);

    /** Same atomic upsert and creation-metadata rules as createTenant. */
    void updateTenant(Tenant tenant);

    Optional<ResolvedTenantContext> getTenantForUser(String userPrincipal);

    /**
     * Trusted provisioning only, without caller authorization; sets the single membership/role.
     * SQL uses mutateMembership.
     */
    void addUserToTenant(String tenantId, String userPrincipal, TenantRole role);

    /** Trusted provisioning only; removes only the named tenant membership. SQL uses mutateMembership. */
    void removeUserFromTenant(String tenantId, String userPrincipal);
}
