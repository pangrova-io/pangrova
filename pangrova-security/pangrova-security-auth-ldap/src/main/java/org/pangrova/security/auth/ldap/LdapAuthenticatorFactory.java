package org.pangrova.security.auth.ldap;

import org.pangrova.security.api.authentication.Authenticator;
import org.pangrova.security.api.authentication.AuthenticatorFactory;

import java.util.Map;

/** Installed Coordinator factory for the LDAP authenticator. */
public final class LdapAuthenticatorFactory implements AuthenticatorFactory {
    private static final Map<String, String> CONFIGURATION_KEYS = Map.ofEntries(
            Map.entry("ldap-url", "ldapUrl"), Map.entry("base-dn", "baseDn"),
            Map.entry("user-search-filter", "userSearchFilter"), Map.entry("user-dn-pattern", "userDnPattern"),
            Map.entry("bind-dn", "bindDn"), Map.entry("bind-password", "bindPassword"),
            Map.entry("group-search-base", "groupSearchBase"), Map.entry("group-search-filter", "groupSearchFilter"),
            Map.entry("group-attribute", "groupAttribute"), Map.entry("connect-timeout-ms", "connectTimeoutMs"),
            Map.entry("read-timeout-ms", "readTimeoutMs"));

    @Override
    public String providerId() {
        return "LDAP";
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
        if (properties.getOrDefault("ldapUrl", "").isBlank()) {
            throw new IllegalArgumentException("LDAP requires security.auth.properties.ldap-url");
        }
    }

    @Override
    public Authenticator create() {
        return new LdapAuthenticator();
    }
}
