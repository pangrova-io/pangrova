package org.pangrova.security.api.identity;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Immutable authenticated identity. Authentication implementations establish its claims;
 * authorization resolves current roles and permissions separately.
 */
@Experimental
public record UserIdentity(
        String principal,
        Set<String> groups,
        Map<String, String> attributes,
        Instant authenticatedAt
) {
    public UserIdentity {
        groups = groups == null ? Set.of() : Set.copyOf(groups);
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
