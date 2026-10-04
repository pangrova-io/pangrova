package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.YamlConfigLoader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real configuration translation must preserve internal authentication and speculation settings. */
class CoordinatorProcessConfigurationTest {
    @Test
    void metadataCredentialsReachTheActualStartupConfigWithoutTrimming(@TempDir Path directory)
            throws Exception {
        Path file = directory.resolve("coordinator.yml");
        Files.writeString(file, """
                metadata:
                  jdbc-url: jdbc:postgresql://database.invalid/pangrova_meta
                  jdbc-username: 'database_owner'
                  jdbc-password: ' 密码:with&reserved=characters '
                """);
        var configuration = PangrovaConfiguration.fromYaml(YamlConfigLoader.load(file));
        var bootstrap = PangrovaCoordinatorMain.buildBootstrapConfig(configuration);
        var jdbc = bootstrap.metadataJdbcConfig();
        assertThat(jdbc.username()).isEqualTo("database_owner");
        assertThat(jdbc.password()).isEqualTo(" 密码:with&reserved=characters ");
        assertThat(jdbc.jdbcUrl()).isEqualTo("jdbc:postgresql://database.invalid/pangrova_meta");
        assertThat(jdbc.toString()).doesNotContain(jdbc.username(), jdbc.password(), jdbc.jdbcUrl());
        assertThat(bootstrap.toString()).doesNotContain(jdbc.password());
        assertThat(configuration.toString()).doesNotContain(jdbc.password());
    }

    @Test
    void omittedDatabaseCredentialsPreserveTheExistingDriverDefaultMode() {
        var configuration = PangrovaConfiguration.fromYaml(Map.of(
                "metadata.jdbc-url", "jdbc:postgresql://database.invalid/pangrova_meta"));
        var jdbc = PangrovaCoordinatorMain.buildBootstrapConfig(configuration).metadataJdbcConfig();
        assertThat(jdbc.username()).isEmpty();
        assertThat(jdbc.password()).isEmpty();
    }

    @Test
    void actualYamlCarriesSecretAndSpeculationToTheProductionStartupGate(@TempDir Path directory)
            throws Exception {
        String secret = "a-cluster-secret-with-at-least-32-bytes";
        Path file = directory.resolve("coordinator.yml");
        Files.writeString(file, """
                coordinator:
                  internal-auth-secret: '%s'
                  speculation:
                    enabled: true
                security:
                  enabled: true
                  auth:
                    type: LDAP
                    properties:
                      ldap-url: ldaps://directory.invalid
                """.formatted(secret));
        var configuration = PangrovaConfiguration.fromYaml(YamlConfigLoader.load(file));
        var bootstrap = PangrovaCoordinatorMain.buildBootstrapConfig(configuration);
        assertThat(bootstrap.internalAuthSecret()).isEqualTo(secret);
        assertThat(bootstrap.securityConfig().internalAuthSecret()).isEqualTo(secret);
        assertThat(bootstrap.securityConfig().toString()).doesNotContain(secret);
        assertThat(bootstrap.securityEnabled()).isTrue();
        assertThat(bootstrap.speculationEnabled()).isTrue();
        assertThatCode(() -> PangrovaCoordinatorMain.validateProductionInternalAuth(bootstrap))
                .doesNotThrowAnyException();
    }

    @Test
    void clientAuthenticationDisabledDoesNotDiscardInternalAuthentication() {
        String secret = "a-cluster-secret-with-at-least-32-bytes";
        var bootstrap = configured(secret);
        assertThat(bootstrap.securityEnabled()).isFalse();
        assertThat(bootstrap.internalAuthSecret()).isEqualTo(secret);
        assertThat(bootstrap.speculationEnabled()).isFalse();
        assertThatCode(() -> PangrovaCoordinatorMain.validateProductionInternalAuth(bootstrap))
                .doesNotThrowAnyException();
    }

    @Test
    void productionGateRejectsAbsentBlankAndShortSecretsWithoutDisclosingThem() {
        var absent = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of()));
        assertThatThrownBy(() -> PangrovaCoordinatorMain.validateProductionInternalAuth(absent))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("at least 32 bytes");
        for (String secret : java.util.List.of("", " ", " ".repeat(32), "a".repeat(31))) {
            assertThatThrownBy(() -> PangrovaCoordinatorMain.validateProductionInternalAuth(configured(secret)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("at least 32 bytes");
        }
    }

    @Test
    void productionGateUsesUtf8BytesAndDoesNotTrimSecretIdentity() {
        String secret = "\u754c".repeat(11);
        assertThatCode(() -> PangrovaCoordinatorMain.validateProductionInternalAuth(configured(secret)))
                .doesNotThrowAnyException();
        assertThat(configured(" " + secret + " ").internalAuthSecret()).isEqualTo(" " + secret + " ");
        assertThatThrownBy(() -> PangrovaCoordinatorMain.validateProductionInternalAuth(configured("\u754c".repeat(10))))
                .isInstanceOf(IllegalStateException.class);
    }

    private static CoordinatorBootstrapConfig configured(String secret) {
        return PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.internal-auth-secret", secret)));
    }
}
