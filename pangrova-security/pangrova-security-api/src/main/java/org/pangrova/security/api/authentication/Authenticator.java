package org.pangrova.security.api.authentication;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;

/**
 * SPI for pluggable authentication backends (LDAP, OAuth2, Kerberos, etc.).
 */
@Experimental
public interface Authenticator extends AutoCloseable {

    /** Unique name identifying this authenticator (e.g. "ldap", "oauth2"). */
    String getName();

    /** Initialize with backend-specific configuration. */
    void init(Map<String, String> config);

    /** Attempt to authenticate the given request. */
    AuthenticationResult authenticate(AuthenticationRequest request);

    /** Returns true if this authenticator can handle the given request. */
    boolean supports(AuthenticationRequest request);

    /** Health check for monitoring. */
    boolean isHealthy();

    /**
     * Whether this authenticator deliberately accepts anonymous requests.
     *
     * <p>Wiring code uses this capability instead of inspecting concrete
     * authenticator types when deciding whether to raise the anonymous-access
     * alarm and inject the bypass opt-in flag.</p>
     */
    default boolean bypassesAuthentication() {
        return false;
    }

    @Override
    void close();
}
