package org.pangrova.testing.integration.e2e.config;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.constants.CoordinatorOptions;
import org.pangrova.config.YamlConfigLoader;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for YAML configuration loading.
 *
 * <p>Writes a custom YAML file, loads it via {@link YamlConfigLoader},
 * builds an {@link PangrovaConfiguration}, and verifies all config values
 * (feature flags, JDBC URL, timeouts) match the YAML values.
 *
 * <p>Implements: R9.4
 */
@DisplayName("Config Loading E2E — Custom YAML Verification")
class ConfigLoadingE2ETest extends SqlE2ETestBase {

    @TempDir
    Path configDir;

    private PangrovaConfiguration config;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                "iceberg_catalog", "default", icebergWarehouse().toString()));
    }

    @BeforeEach
    void writeAndLoadCustomYaml() throws IOException {
        Path customYaml = configDir.resolve("custom-coordinator.yml");
        Files.writeString(customYaml, buildCustomYaml());

        Map<String, String> flatMap = YamlConfigLoader.load(customYaml);
        config = PangrovaConfiguration.fromYaml(flatMap);
    }

    // -----------------------------------------------------------------------
    // Feature flags
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Feature Flag Values")
    class FeatureFlagValues {

        @Test
        @DisplayName("All feature flags match YAML values")
        void featureFlagsMatchYaml() {
            assertThat(config.getBoolean(CoordinatorOptions.METADATA_ENABLED)).isTrue();
            assertThat(config.getBoolean(CoordinatorOptions.GOVERNANCE_ENABLED)).isFalse();
            assertThat(config.getBoolean(CoordinatorOptions.RESILIENCE_ENABLED)).isTrue();
            assertThat(config.getBoolean(CoordinatorOptions.STREAMING_ENABLED)).isFalse();
            assertThat(config.getBoolean(CoordinatorOptions.TENANT_ENABLED)).isTrue();
            assertThat(config.getBoolean(CoordinatorOptions.INDEXING_ENABLED)).isTrue();
            assertThat(config.getBoolean(CoordinatorOptions.SECURITY_ENABLED)).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Server configuration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Server Configuration Values")
    class ServerConfigValues {

        @Test
        @DisplayName("Coordinator ID matches YAML")
        void coordinatorIdMatchesYaml() {
            assertThat(config.getString(CoordinatorOptions.COORDINATOR_ID))
                    .isEqualTo("test-coordinator-99");
        }

        @Test
        @DisplayName("Coordinator host and port match YAML")
        void hostAndPortMatchYaml() {
            assertThat(config.getString(CoordinatorOptions.COORDINATOR_HOST))
                    .isEqualTo("0.0.0.0");
            assertThat(config.getInt(CoordinatorOptions.COORDINATOR_PORT))
                    .isEqualTo(9090);
        }
    }

    // -----------------------------------------------------------------------
    // Metadata JDBC configuration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Metadata JDBC Values")
    class MetadataJdbcValues {

        @Test
        @DisplayName("Metadata JDBC URL matches YAML")
        void metadataJdbcUrlMatchesYaml() {
            assertThat(config.getString(CoordinatorOptions.METADATA_JDBC_URL))
                    .isEqualTo("jdbc:postgresql://testhost:5432/pangrova_meta");
        }

    }

    // -----------------------------------------------------------------------
    // Timeout configuration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Timeout Values")
    class TimeoutValues {

        @Test
        @DisplayName("Query timeout matches YAML")
        void queryTimeoutMatchesYaml() {
            assertThat(config.getDuration(CoordinatorOptions.QUERY_TIMEOUT))
                    .isEqualTo(Duration.ofMinutes(10));
        }

        @Test
        @DisplayName("Worker heartbeat timeout matches YAML")
        void workerHeartbeatTimeoutMatchesYaml() {
            assertThat(config.getDuration(CoordinatorOptions.WORKER_HEARTBEAT_TIMEOUT))
                    .isEqualTo(Duration.ofSeconds(60));
        }

        @Test
        @DisplayName("Max concurrent queries matches YAML")
        void maxConcurrentQueriesMatchesYaml() {
            assertThat(config.getInt(CoordinatorOptions.QUERY_MAX_CONCURRENT))
                    .isEqualTo(200);
        }
    }

    // -----------------------------------------------------------------------
    // Classpath defaults merge
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Classpath Defaults Merge")
    class ClasspathDefaultsMerge {

        @Test
        @DisplayName("Custom YAML overrides classpath defaults")
        void customOverridesClasspathDefaults() throws IOException {
            Path overrideYaml = configDir.resolve("override.yml");
            Files.writeString(overrideYaml, buildCustomYaml());

            Map<String, String> merged = YamlConfigLoader.loadWithDefaults(
                    overrideYaml, "coordinator.yml");
            PangrovaConfiguration mergedConfig = PangrovaConfiguration.fromYaml(merged);

            assertThat(mergedConfig.getString(CoordinatorOptions.COORDINATOR_ID))
                    .isEqualTo("test-coordinator-99");
            assertThat(mergedConfig.getInt(CoordinatorOptions.COORDINATOR_PORT))
                    .isEqualTo(9090);
        }
    }

    // -----------------------------------------------------------------------
    // Default values when key absent
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Default Values")
    class DefaultValues {

        @Test
        @DisplayName("Missing keys fall back to ConfigOption defaults")
        void missingKeysFallBackToDefaults() throws IOException {
            Path minimalYaml = configDir.resolve("minimal.yml");
            Files.writeString(minimalYaml, "coordinator:\n  id: minimal-coord\n");

            Map<String, String> flatMap = YamlConfigLoader.load(minimalYaml);
            PangrovaConfiguration minConfig = PangrovaConfiguration.fromYaml(flatMap);

            // Explicit value
            assertThat(minConfig.getString(CoordinatorOptions.COORDINATOR_ID))
                    .isEqualTo("minimal-coord");
            // Default values for absent keys
            assertThat(minConfig.getInt(CoordinatorOptions.COORDINATOR_PORT))
                    .isEqualTo(8080);
            assertThat(minConfig.getBoolean(CoordinatorOptions.SECURITY_ENABLED))
                    .isFalse();
            assertThat(minConfig.getBoolean(CoordinatorOptions.METADATA_ENABLED))
                    .isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // YAML builder
    // -----------------------------------------------------------------------

    private String buildCustomYaml() {
        return """
                coordinator:
                  id: test-coordinator-99
                  host: 0.0.0.0
                  port: 9090
                  query:
                    timeout: 10m
                    max-concurrent: 200
                  worker:
                    heartbeat-timeout: 60s
                    max-retry: 5
                metadata:
                  enabled: true
                  jdbc-url: jdbc:postgresql://testhost:5432/pangrova_meta
                  cache:
                    l1-max-size: 20000
                    l1-ttl: 120s
                governance:
                  enabled: false
                resilience:
                  enabled: true
                streaming:
                  enabled: false
                tenant:
                  enabled: true
                indexing:
                  enabled: true
                security:
                  enabled: false
                """;
    }
}
