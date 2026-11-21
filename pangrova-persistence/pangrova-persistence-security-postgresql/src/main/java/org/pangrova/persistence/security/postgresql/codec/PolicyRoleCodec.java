package org.pangrova.persistence.security.postgresql.codec;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/** Canonical encoding for the existing persisted security policy role-list fields. */
@Internal
public final class PolicyRoleCodec {
    private PolicyRoleCodec() {}

    /** Joins a role list to the comma-separated form stored in {@code apply_to_roles}/{@code exempt_roles}. */
    public static String encodeRoles(List<String> roles) {
        return roles == null || roles.isEmpty() ? "" : String.join(",", roles);
    }

    /** Parses the comma-separated stored role form back into a list ('' → empty list). */
    public static List<String> decodeRoles(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return List.of();
        }
        return List.of(encoded.split(","));
    }
}
