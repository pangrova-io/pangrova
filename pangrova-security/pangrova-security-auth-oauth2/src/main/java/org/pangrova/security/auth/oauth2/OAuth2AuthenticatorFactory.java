package org.pangrova.security.auth.oauth2;

import org.pangrova.security.api.authentication.Authenticator;
import org.pangrova.security.api.authentication.AuthenticatorFactory;

import java.util.Map;

/** Installed Coordinator factory for the OAUTH2 authenticator. */
public final class OAuth2AuthenticatorFactory implements AuthenticatorFactory {
    private static final Map<String, String> CONFIGURATION_KEYS = Map.of("issuer-uri", "issuerUri", "audience", "audience");

    @Override
    public String providerId() {
        return "OAUTH2";
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
        if (properties.getOrDefault("issuerUri", "").isBlank()) {
            throw new IllegalArgumentException("OAuth2 requires security.auth.properties.issuer-uri");
        }
    }

    @Override
    public Authenticator create() {
        return new OAuth2Authenticator();
    }
}
