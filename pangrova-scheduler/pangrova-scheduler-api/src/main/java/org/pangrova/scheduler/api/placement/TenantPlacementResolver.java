package org.pangrova.scheduler.api.placement;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/**
 * Resolves current tenant placement policy, independently of the tenant identity.
 * Empty means explicitly shared, unassigned workers; a value names a dedicated pool.
 * Unknown/inactive tenants and authority read failures must throw, never become shared.
 * Implementations are thread-safe and borrow their authority; Core does not close it.
 * This lookup is not admission, authorization, or a distributed resource reservation.
 */
@FunctionalInterface
@Experimental
public interface TenantPlacementResolver {
    Optional<String> resourcePool(String tenantId);
}
