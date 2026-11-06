package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.core.security.ColumnMaskApplier;
import org.pangrova.planner.core.security.RowFilterApplier;

/**
 * Groups security-related dependencies for QueryDispatcher.
 * All fields are nullable — null means the corresponding security feature is disabled.
 */
public record SecurityContextBundle(
        AccessControlEnforcer accessControlEnforcer,
        ColumnMaskApplier columnMaskApplier,
        RowFilterApplier rowFilterApplier) {

    /** Convenience factory for when no security features are configured. */
    public static SecurityContextBundle none() {
        return new SecurityContextBundle(null, null, null);
    }
}
