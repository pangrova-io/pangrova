package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.config.CoordinatorOptions;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.security.api.authentication.AuthenticatorFactory;
import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig;

import java.util.Map;
import java.util.stream.Collectors;

/** Loads process authentication settings from YAML; permissions are owned by persistent SQL state. */
final class SecurityConfigurationLoader {
    private static final String AUTH_PROPERTIES = "security.auth.properties.";

    private SecurityConfigurationLoader() {}

    static SecurityConfig load(PangrovaConfiguration config) {
        rejectUnsupportedSecurityKeys(config.toMap());
        String internalSecret = config.getString(CoordinatorOptions.INTERNAL_AUTH_SECRET);
        if (!config.getBoolean(CoordinatorOptions.SECURITY_ENABLED)) {
            return internalSecret == null ? null
                    : SecurityConfig.internalRequestsOnly(internalSecret);
        }
        String type = config.getString(CoordinatorOptions.SECURITY_AUTH_TYPE);
        if (type == null || type.isBlank()) {
            throw invalid("security.auth.type is required when security.enabled=true");
        }
        AuthenticatorFactory factory = CoordinatorAuthenticatorFactories.select(type);
        return new SecurityConfig(factory, internalSecret, authenticatorProperties(config.toMap(), factory));
    }

    private static Map<String, String> authenticatorProperties(
            Map<String, String> configuration, AuthenticatorFactory factory) {
        Map<String, String> names = factory.configurationKeys();
        return configuration.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(AUTH_PROPERTIES))
                .collect(Collectors.toUnmodifiableMap(
                        entry -> backendPropertyName(entry.getKey(), names), Map.Entry::getValue));
    }

    private static String backendPropertyName(String key, Map<String, String> names) {
        String name = names.get(key.substring(AUTH_PROPERTIES.length()));
        if (name == null) {
            throw invalid("Unsupported authenticator property: " + key);
        }
        return name;
    }

    private static void rejectUnsupportedSecurityKeys(Map<String, String> configuration) {
        configuration.keySet().stream()
                .filter(key -> key.equals("security.grants") || key.startsWith("security.grants."))
                .findFirst().ifPresent(key -> {
                    throw invalid("Unsupported security configuration: " + key
                            + "; provision roles and grants in the authoritative PostgreSQL store");
                });
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE, message);
    }
}
