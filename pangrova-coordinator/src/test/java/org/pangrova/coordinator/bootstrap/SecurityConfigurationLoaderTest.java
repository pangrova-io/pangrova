package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.YamlConfigLoader;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecurityConfigurationLoaderTest {
    @Test
    void loadsActualYamlBackendProperties(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("coordinator.yml");
        Files.writeString(file, """
                security:
                  enabled: true
                  auth:
                    type: OAUTH2
                    properties:
                      issuer-uri: http://127.0.0.1:9
                      audience: pangrova
                """);
        var security = SecurityConfigurationLoader.load(PangrovaConfiguration.fromYaml(YamlConfigLoader.load(file)));
        assertThat(security.authenticatorType()).isEqualTo(AuthenticatorType.OAUTH2);
        assertThat(security.authenticatorProperties()).containsExactlyInAnyOrderEntriesOf(
                Map.of("issuerUri", "http://127.0.0.1:9", "audience", "pangrova"));
        assertThatThrownBy(() -> security.authenticatorProperties().put("audience", "other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void ldapPropertiesReachTheBackendWithoutExposingCredentials() {
        var security = load(Map.of("security.enabled", "true", "security.auth.type", "LDAP",
                "security.auth.properties.ldap-url", "ldaps://directory.invalid",
                "security.auth.properties.bind-password", "sensitive-value"));
        assertThat(security.authenticatorProperties()).containsEntry("bindPassword", "sensitive-value");
        assertThat(security.toString()).doesNotContain("sensitive-value", "ldaps://directory.invalid");
    }

    @Test
    void enabledAuthenticationRequiresIssuerOrDirectory() {
        assertThatThrownBy(() -> load(Map.of("security.enabled", "true", "security.auth.type", "OAUTH2")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("issuer-uri");
        assertThatThrownBy(() -> load(Map.of("security.enabled", "true", "security.auth.type", "LDAP")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ldap-url");
    }

    @Test
    void unknownAndCrossProviderPropertiesAreRejected() {
        for (String property : java.util.List.of("issuerUri", "ldap-url", "require-signature-verification")) {
            assertThatThrownBy(() -> load(Map.of("security.enabled", "true", "security.auth.type", "OAUTH2",
                    "security.auth.properties." + property, "false")))
                    .isInstanceOf(PangrovaException.class).hasMessageContaining("Unsupported authenticator property");
        }
    }

    @Test
    void configGrantsAreRejectedInsteadOfOverridingTheDatabase() {
        assertThatThrownBy(() -> load(Map.of("security.enabled", "true", "security.auth.type", "NOOP",
                "security.grants", "[{role: admin}]")))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Unsupported security configuration");
        assertThatThrownBy(() -> load(Map.of("security.enabled", "true", "security.auth.type", "NOOP",
                "security.grants.reader.typo", "SELECT")))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Unsupported security configuration");
    }

    @Test
    void configGrantsAreRejectedEvenWhenSecurityIsDisabled() {
        assertThatThrownBy(() -> load(Map.of("security.grants.reader.role", "admin")))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Unsupported security configuration");
    }

    @Test
    void disabledSecurityHasNoImplicitAuthenticator() {
        assertThat(load(Map.of())).isNull();
    }

    private static CoordinatorBootstrapConfig.SecurityConfig load(Map<String, String> values) {
        return SecurityConfigurationLoader.load(PangrovaConfiguration.fromYaml(values));
    }
}
