package org.pangrova.security.authz;

import java.time.Instant;

/**
 * IdP-group → Pangrova-role mapping. Persisted in {@code pangrova_group_roles}.
 *
 * <p>This mapping is the formal seam between identity and authorization. Earlier
 * versions of the access-control code conflated the two by treating an IdP group
 * as if it were a role; this caused per-coordinator divergence (each coordinator
 * could load a different static config) and made it impossible to refactor the
 * RBAC model without touching the IdP. Resolving the two at the Pangrova layer
 * keeps roles authoritative inside the engine.
 *
 * @param groupName the IdP-supplied group (matches an entry in {@link org.pangrova.spi.security.UserIdentity#groups()})
 * @param roleName  Pangrova role granted to members of the group (FK → {@link Role#name()})
 * @param mappedBy  principal that established the mapping
 * @param mappedAt  mapping timestamp
 */
public record GroupRoleMapping(
        String groupName,
        String roleName,
        String mappedBy,
        Instant mappedAt
) {

    public GroupRoleMapping {
        if (groupName == null || groupName.isBlank()) {
            throw new IllegalArgumentException("groupName must not be null or blank");
        }
        if (roleName == null || roleName.isBlank()) {
            throw new IllegalArgumentException("roleName must not be null or blank");
        }
    }

    public static GroupRoleMapping of(String groupName, String roleName) {
        return new GroupRoleMapping(groupName, roleName, null, Instant.now());
    }
}
