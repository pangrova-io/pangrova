package org.pangrova.spi.security;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI contract exposing resolved tenant membership for a user.
 * <p>
 * Implementations reside in pangrova-security/tenant/.
 * Consumers (e.g. pangrova-coordinator) depend only on this interface.
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
