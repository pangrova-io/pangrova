package org.pangrova.transport.client.authentication;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Immutable credentials for the sole REST authorization header; diagnostic output is redacted. */
public final class ClientAuthentication {
    public enum Scheme { BEARER, BASIC }

    private final Scheme scheme;
    private final String authorizationHeader;

    private ClientAuthentication(Scheme scheme, String authorizationHeader) {
        this.scheme = scheme;
        this.authorizationHeader = authorizationHeader;
    }

    /**
     * Creates an authorization policy for an HTTP bearer token.
     *
     * @param token token satisfying the HTTP bearer-token syntax
     * @return the redacted credential policy
     */
    public static ClientAuthentication bearer(String token) {
        Objects.requireNonNull(token, "bearer token");
        if (!token.matches("[A-Za-z0-9._~+/-]+=*")) {
            throw new IllegalArgumentException("Bearer token must use the HTTP bearer token syntax");
        }
        return new ClientAuthentication(Scheme.BEARER, "Bearer " + token);
    }

    /**
     * Creates a UTF-8 HTTP Basic authorization policy.
     *
     * @param username nonblank username without colons or control characters
     * @param password password without control characters
     * @return the redacted credential policy
     */
    public static ClientAuthentication basic(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        if (username.isBlank() || username.indexOf(':') >= 0
                || hasControl(username) || hasControl(password)) {
            throw new IllegalArgumentException("Basic credentials contain an invalid username or control character");
        }
        String encoded = Base64.getEncoder().encodeToString(
                (username + ':' + password).getBytes(StandardCharsets.UTF_8));
        return new ClientAuthentication(Scheme.BASIC, "Basic " + encoded);
    }

    private static boolean hasControl(String value) {
        return value.codePoints().anyMatch(Character::isISOControl);
    }

    public Scheme scheme() { return scheme; }

    /** Only the actual HTTP request builder should consume this credential-bearing value. */
    public String authorizationHeader() { return authorizationHeader; }

    @Override public String toString() { return "ClientAuthentication[scheme=" + scheme + ", credentials=<redacted>]"; }
}
