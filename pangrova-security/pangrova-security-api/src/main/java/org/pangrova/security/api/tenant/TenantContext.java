package org.pangrova.security.api.tenant;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Caller contract exposing resolved tenant membership for a user.
 * {@link ResolvedTenantContext} is the immutable API representation; tenant resolution
 * and current membership checks remain in the owning services.
 */
@Experimental
public interface TenantContext {

    /**
     * Unique identifier of the tenant.
     */
    String tenantId();

    /**
     * Display name of the tenant.
     */
    String tenantName();

    /**
     * Whether the tenant is currently active (not suspended or deleted).
     */
    boolean isActive();

    /**
     * Whether the tenant is suspended.
     */
    boolean isSuspended();

    /**
     * Role the user holds within this tenant (e.g. "OWNER", "ADMIN", "MEMBER").
     */
    String roleName();
}
