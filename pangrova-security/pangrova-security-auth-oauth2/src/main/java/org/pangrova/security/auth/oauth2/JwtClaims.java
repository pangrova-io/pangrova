package org.pangrova.security.auth.oauth2;

import java.util.Set;

/** Verified OIDC claims used to authenticate a principal. A missing expiration is represented by zero. */
record JwtClaims(String issuer, String subject, Set<String> audiences, long expiration, Set<String> groups) {
    JwtClaims {
        audiences = audiences == null ? Set.of() : Set.copyOf(audiences);
        groups = Set.copyOf(groups);
    }
}
