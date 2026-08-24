package org.pangrova.operations.diagnostics;

import java.util.Objects;
import java.util.Set;

/**
 * Identifies the caller of a diagnostic operation. Used for authorization checks
 * on sensitive endpoints (e.g., heap dump).
 *
 * @param principal the authenticated user principal name
 * @param roles     the set of roles assigned to the caller
 */
public record CallerContext(String principal, Set<String> roles) {

    public CallerContext {
        Objects.requireNonNull(principal, "principal");
        roles = roles == null ? Set.of() : Set.copyOf(roles);
    }

    /**
     * Checks whether this caller has the specified role.
     */
    public boolean hasRole(String role) {
        return roles.contains(role);
    }
}
