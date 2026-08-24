package org.pangrova.security.authz;

import java.time.Instant;

/**
 * Direct user → role binding. Persisted in {@code pangrova_user_roles}.
 *
 * <p>Resolved at access-control time as one of the two channels that turn an authenticated
 * principal into the set of effective roles whose grants will be checked. The other channel
 * is {@link GroupRoleMapping}.
 *
 * @param userId    the principal (matches {@link org.pangrova.spi.security.UserIdentity#principal()})
 * @param roleName  the role granted to that user (FK → {@link Role#name()})
 * @param grantedBy principal that issued the grant (admin user / system)
 * @param grantedAt grant timestamp
 */
public record UserRoleAssignment(
        String userId,
        String roleName,
        String grantedBy,
        Instant grantedAt
) {

    public UserRoleAssignment {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId must not be null or blank");
        }
        if (roleName == null || roleName.isBlank()) {
            throw new IllegalArgumentException("roleName must not be null or blank");
        }
    }

    public static UserRoleAssignment of(String userId, String roleName) {
        return new UserRoleAssignment(userId, roleName, null, Instant.now());
    }
}
