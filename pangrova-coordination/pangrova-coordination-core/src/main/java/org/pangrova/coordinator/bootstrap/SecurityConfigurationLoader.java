package org.pangrova.coordinator.bootstrap;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.config.CoordinatorOptions;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Loads process authentication settings from YAML; permissions are owned by persistent SQL state. */
final class SecurityConfigurationLoader {
    private static final String AUTH_PROPERTIES = "security.auth.properties.";
    private static final Map<String, String> OAUTH_PROPERTIES = Map.of(
            "issuer-uri", "issuerUri", "audience", "audience");
    private static final Map<String, String> LDAP_PROPERTIES = Map.ofEntries(
            Map.entry("ldap-url", "ldapUrl"), Map.entry("base-dn", "baseDn"),
            Map.entry("user-search-filter", "userSearchFilter"), Map.entry("user-dn-pattern", "userDnPattern"),
            Map.entry("bind-dn", "bindDn"), Map.entry("bind-password", "bindPassword"),
            Map.entry("group-search-base", "groupSearchBase"), Map.entry("group-search-filter", "groupSearchFilter"),
            Map.entry("group-attribute", "groupAttribute"), Map.entry("connect-timeout-ms", "connectTimeoutMs"),
            Map.entry("read-timeout-ms", "readTimeoutMs"));

    private SecurityConfigurationLoader() {}

    static SecurityConfig load(PangrovaConfiguration config) {
        rejectUnsupportedSecurityKeys(config.toMap());
        String internalSecret = config.getString(CoordinatorOptions.INTERNAL_AUTH_SECRET);
        if (!config.getBoolean(CoordinatorOptions.SECURITY_ENABLED)) {
            return internalSecret == null ? null
                    : new SecurityConfig(AuthenticatorType.NOOP, internalSecret, Map.of());
        }
        String type = config.getString(CoordinatorOptions.SECURITY_AUTH_TYPE);
        if (type == null || type.isBlank()) {
            throw invalid("security.auth.type is required when security.enabled=true");
        }
        AuthenticatorType authenticator;
        try {
            authenticator = AuthenticatorType.valueOf(type.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException failure) {
            throw invalid("Unknown security.auth.type: " + type);
        }
        return new SecurityConfig(authenticator, internalSecret,
                authenticatorProperties(config.toMap(), authenticator));
    }

    private static Map<String, String> authenticatorProperties(
            Map<String, String> configuration, AuthenticatorType type) {
        Map<String, String> names = switch (type) {
            case OAUTH2 -> OAUTH_PROPERTIES;
            case LDAP -> LDAP_PROPERTIES;
            case NOOP -> Map.of();
        };
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
