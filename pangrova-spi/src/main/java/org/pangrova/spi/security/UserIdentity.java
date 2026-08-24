package org.pangrova.spi.security;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Immutable identity of an authenticated user.
 * Stable SPI contract — implementations reside in pangrova-security.
 */
/**
 * Immutable identity of an authenticated user.
 * Stable SPI contract — implementations reside in pangrova-security.
 */
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
