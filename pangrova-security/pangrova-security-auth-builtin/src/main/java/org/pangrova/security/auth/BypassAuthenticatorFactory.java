package org.pangrova.security.auth;

import org.pangrova.security.api.authentication.Authenticator;
import org.pangrova.security.api.authentication.AuthenticatorFactory;

import java.util.Map;

/** Installed Coordinator factory for the NOOP authenticator. */
public final class BypassAuthenticatorFactory implements AuthenticatorFactory {
    private static final Map<String, String> CONFIGURATION_KEYS = Map.of();

    @Override
    public String providerId() {
        return "NOOP";
    }

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public Map<String, String> configurationKeys() {
        return CONFIGURATION_KEYS;
    }

    @Override
    public void validate(Map<String, String> properties) {
        if (!properties.isEmpty()) {
            throw new IllegalArgumentException("NOOP accepts no authenticator properties");
        }
    }

    @Override
    public Authenticator create() {
        return new BypassAuthenticator();
    }
}
