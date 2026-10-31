package org.pangrova.security.api.policy;

import java.time.Instant;

/**
 * A role is the unit of privilege grouping in the Pangrova RBAC model.
 *
 * <p>Roles are <strong>distinct from groups</strong>. A group is an identity-layer
 * concept supplied by the IdP (LDAP / OIDC); a role is an Pangrova-internal RBAC
 * primitive that owns privileges. Identities map to roles in two ways:
 *
 * <ul>
 *     <li>Direct user → role bindings (see {@link UserRoleAssignment})</li>
 *     <li>Group → role mappings (see {@link GroupRoleMapping})</li>
 * </ul>
 *
 * <p>Persisted in {@code pangrova_roles} via {@link org.pangrova.security.store.jdbc.PersistentRoleStore}.
 *
 * @param name        canonical role name (case-sensitive); primary key
 * @param description optional human-readable description
 * @param createdBy   principal that created the role (may be {@code null} for boot-time roles)
 * @param createdAt   creation timestamp
 */
public record Role(
        String name,
        String description,
        String createdBy,
        Instant createdAt
) {

    public Role {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("role name must not be null or blank");
        }
    }

    /** Convenience factory for tests / boot-time defaults. */
    public static Role of(String name) {
        return new Role(name, null, null, Instant.now());
    }
}
