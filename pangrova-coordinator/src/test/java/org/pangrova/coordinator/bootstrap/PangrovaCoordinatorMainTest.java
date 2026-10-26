package org.pangrova.coordinator.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void buildBootstrapConfig_defaultsDisableTenancyWithoutSecurity() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.metadataEnabled()).isTrue();
        assertThat(result.governanceEnabled()).isTrue();
        assertThat(result.resilienceEnabled()).isTrue();
        assertThat(result.streamingEnabled()).isTrue();
        assertThat(result.tenantEnabled()).isFalse();
        assertThat(result.operationsEnabled()).isTrue();
        assertThat(result.observabilityEnabled()).isTrue();
        assertThat(result.securityEnabled()).isFalse();
    }

    @Test
    void buildBootstrapConfig_rejectsTenancyWithoutAuthentication() {
        PangrovaConfiguration noSecurity = PangrovaConfiguration.fromYaml(
                Map.of("tenant.enabled", "true"));
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(noSecurity))
                .hasMessageContaining("requires security.enabled=true");

        PangrovaConfiguration noOpSecurity = PangrovaConfiguration.fromYaml(Map.of(
                "tenant.enabled", "true", "security.enabled", "true",
                "security.auth.type", "NOOP"));
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(noOpSecurity))
                .hasMessageContaining("cannot use the NOOP authenticator");
    }

    @Test
    void buildBootstrapConfig_allowsTenancyWithAuthenticatedSecurity() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "tenant.enabled", "true", "security.enabled", "true",
                "security.auth.type", "OAUTH2",
                "security.auth.properties.issuer-uri", "http://127.0.0.1:9"));

        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.tenantEnabled()).isTrue();
        assertThat(result.securityEnabled()).isTrue();
    }

    @Test
    void buildBootstrapConfig_usesDefaultCoordinatorIdAndPort() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.coordinatorId()).isEqualTo("coordinator-1");
        assertThat(result.httpPort()).isEqualTo(8080);
        assertThat(result.httpHost()).isEqualTo("0.0.0.0");
    }

    @Test
    void buildBootstrapConfig_respectsYamlOverrides() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.id", "my-coordinator",
                "coordinator.port", "9090",
                "coordinator.host", "127.0.0.1",
                "security.enabled", "true",
                "security.auth.type", "OAUTH2",
                "security.auth.properties.issuer-uri", "http://127.0.0.1:9",
                "metadata.enabled", "false"
        ));
        CoordinatorBootstrapConfig result = PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.coordinatorId()).isEqualTo("my-coordinator");
        assertThat(result.httpPort()).isEqualTo(9090);
        assertThat(result.httpHost()).isEqualTo("127.0.0.1");
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
    void buildBootstrapConfigConsumesTransportAddressPortAndBudgetWithoutTls() {
        var configured = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.transport.port", "19091",
                "coordinator.transport.advertised-host", "coordinator.example.test",
                "coordinator.transport.allocator-limit-bytes", "16777216"));
        var result = PangrovaCoordinatorMain.buildBootstrapConfig(configured);
        assertThat(result.coordinatorTransportPort()).isEqualTo(19091);
        assertThat(result.coordinatorTransportAdvertisedHost()).isEqualTo("coordinator.example.test");
        assertThat(result.coordinatorTransportAllocatorLimitBytes()).isEqualTo(16777216);
        assertThat(result.shuffleTlsConfig()).isNull();
    }

    @Test
    void buildBootstrapConfigRejectsUnroutableAddressOrInvalidTransportBudget() {
        for (String value : java.util.List.of("0.0.0.0", "::", "*", " ")) {
            assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(
                    PangrovaConfiguration.fromYaml(Map.of("coordinator.transport.advertised-host", value))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.transport.allocator-limit-bytes", "0"))))
                .hasMessageContaining("must be positive");
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.transport.port", "65536"))))
                .hasMessageContaining("0..65535");
    }

    @Test
    void buildBootstrapConfigLoadsCompleteShuffleTlsMaterial() throws Exception {
        Path certificate = Path.of(getClass()
                .getResource("/tls/localhost-cert.pem").toURI());
        Path privateKey = Path.of(getClass()
                .getResource("/tls/localhost-key.pem").toURI());
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.transport.tls.enabled", "true",
                "coordinator.transport.tls.certificate-chain-path",
                certificate.toString(),
                "coordinator.transport.tls.private-key-path", privateKey.toString(),
                "coordinator.transport.tls.root-ca-path", certificate.toString(),
                "coordinator.shuffle.tls.server-identity", "localhost"));

        CoordinatorBootstrapConfig result =
                PangrovaCoordinatorMain.buildBootstrapConfig(config);

        assertThat(result.shuffleTlsConfig()).isNotNull();
        assertThat(result.shuffleTlsConfig().serverIdentity()).isEqualTo("localhost");
    }

    @Test
    void buildBootstrapConfigRejectsIncompleteShuffleTlsMaterial() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.transport.tls.enabled", "true",
                "coordinator.transport.tls.root-ca-path", "/missing/ca.pem"));

        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(config))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("requires certificate");
    }

    @Test
    void buildBootstrapConfigRejectsTlsMaterialWhenTlsIsDisabled() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.transport.tls.root-ca-path", "/secrets/ca.pem"));

        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(config))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("tls.enabled=true");
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
