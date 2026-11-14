package org.pangrova.security.api.authentication;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;

/** Coordinator authentication provider. Discovering factories must not acquire resources. */
@Experimental
public interface AuthenticatorFactory {
    int API_VERSION = 1;

    /** Stable, uppercase deployment ID. NOOP is reserved for explicit anonymous access. */
    String providerId();

    int apiVersion();

    /** Accepted YAML property names and the corresponding keys passed to the authenticator. */
    Map<String, String> configurationKeys();

    /** Validates backend-specific properties without starting the backend or logging credentials. */
    void validate(Map<String, String> properties);

    /** Returns a fresh uninitialized instance. The caller closes it even if initialization fails. */
    Authenticator create();
}
