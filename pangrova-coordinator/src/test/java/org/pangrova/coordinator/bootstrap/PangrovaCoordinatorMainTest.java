package org.pangrova.coordinator.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PangrovaCoordinatorMain} config loading and CLI arg parsing.
 */
class PangrovaCoordinatorMainTest {

    @Test
    void parseConfigPath_returnsPathWhenPresent() {
        String[] args = {"--config", "/tmp/coordinator.yml", "--other", "value"};
        assertThat(PangrovaCoordinatorMain.parseConfigPath(args)).isEqualTo("/tmp/coordinator.yml");
    }

    @Test
    void parseConfigPath_returnsNullWhenAbsent() {
        String[] args = {"--other", "value"};
        assertThat(PangrovaCoordinatorMain.parseConfigPath(args)).isNull();
    }

    @Test
    void parseConfigPath_returnsNullForEmptyArgs() {
        assertThat(PangrovaCoordinatorMain.parseConfigPath(new String[0])).isNull();
    }

    @Test
    void parseConfigPath_returnsNullWhenConfigIsLastArg() {
        String[] args = {"--config"};
        assertThat(PangrovaCoordinatorMain.parseConfigPath(args)).isNull();
    }

    @Test
    void buildBootstrapConfig_defaultsEnableAllModulesExceptSecurity() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.metadataEnabled()).isTrue();
        assertThat(result.governanceEnabled()).isTrue();
        assertThat(result.resilienceEnabled()).isTrue();
        assertThat(result.streamingEnabled()).isTrue();
        assertThat(result.tenantEnabled()).isTrue();
        assertThat(result.operationsEnabled()).isTrue();
        assertThat(result.observabilityEnabled()).isTrue();
        assertThat(result.securityEnabled()).isFalse();
    }

    @Test
    void buildBootstrapConfig_usesDefaultCoordinatorIdAndPort() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.coordinatorId()).isEqualTo("coordinator-1");
        assertThat(result.httpPort()).isEqualTo(8080);
    }

    @Test
    void buildBootstrapConfig_respectsYamlOverrides() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.id", "my-coordinator",
                "coordinator.port", "9090",
                "security.enabled", "true",
                "security.auth.type", "OAUTH2",
                "metadata.enabled", "false"
        ));
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.coordinatorId()).isEqualTo("my-coordinator");
        assertThat(result.httpPort()).isEqualTo(9090);
        assertThat(result.securityEnabled()).isTrue();
        assertThat(result.metadataEnabled()).isFalse();
    }

    @Test
    void buildBootstrapConfig_queryTimeoutFromDuration() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.query.timeout", "10m"
        ));
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.queryTimeoutSeconds()).isEqualTo(600);
    }

    @Test
    void loadPangrovaConfiguration_loadsClasspathDefaultsWhenNoConfigArg(@TempDir Path tempDir)
            throws IOException {
        PangrovaConfiguration config = PangrovaCoordinatorMain.loadPangrovaConfiguration(new String[0]);

        assertThat(config).isNotNull();
        assertThat(config.toMap()).isNotEmpty();
    }

    @Test
    void loadPangrovaConfiguration_loadsFromCliYaml(@TempDir Path tempDir) throws IOException {
        Path yamlFile = tempDir.resolve("custom.yml");
        Files.writeString(yamlFile, "coordinator:\n  port: 9999\n");

        PangrovaConfiguration config = PangrovaCoordinatorMain.loadPangrovaConfiguration(
                new String[]{"--config", yamlFile.toString()});

        assertThat(config.toMap()).containsEntry("coordinator.port", "9999");
    }

    @Test
    void buildBootstrapConfig_metadataJdbcConfigNullWhenNoUrl() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.metadataJdbcConfig()).isNull();
    }

    @Test
    void buildBootstrapConfig_metadataJdbcConfigSetWhenUrlProvided() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "metadata.jdbc-url", "jdbc:postgresql://localhost:5432/pangrova"
        ));
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.metadataJdbcConfig()).isNotNull();
        assertThat(result.metadataJdbcConfig().jdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/pangrova");
    }
}
