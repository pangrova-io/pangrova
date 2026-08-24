package org.pangrova.security.authn;

import java.util.Map;

/**
 * SPI for pluggable authentication backends (LDAP, OAuth2, Kerberos, etc.).
 */
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

    @Override
    void close();
}
