package org.pangrova.config;

import org.pangrova.config.constants.WorkerOptions;

import org.pangrova.config.constants.CoordinatorOptions;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The production process templates consume the environment names used by cluster deployments. */
class ProcessDeploymentConfigurationTest {
    @Test
    void bundledCoordinatorLoadsSecretAndIdentityFromActualEnvironment() throws Exception {
        Map<String, String> environment = Map.of(
                "PANGROVA_COORDINATOR_ID", "e2e-coordinator",
                "PANGROVA_INTERNAL_AUTH_SECRET", "a-cluster-secret-with-at-least-32-bytes");
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                YamlConfigLoader.loadFromClasspath("coordinator.yml", environment), environment);
        assertThat(config.getString(CoordinatorOptions.COORDINATOR_ID)).isEqualTo("e2e-coordinator");
        assertThat(config.getString(CoordinatorOptions.INTERNAL_AUTH_SECRET))
                .isEqualTo(environment.get("PANGROVA_INTERNAL_AUTH_SECRET"));
        assertThat(config.getBoolean(CoordinatorOptions.SPECULATION_ENABLED)).isFalse();
    }

    @Test
    void productionCoordinatorTemplateBindsTheSameInternalSecretAsWorkers() throws Exception {
        var environment = Map.of("PANGROVA_DB_HOST", "postgres",
                "PANGROVA_DB_USERNAME", "metadata_reader",
                "PANGROVA_DB_PASSWORD", "a-metadata-password-with:reserved&characters",
                "PANGROVA_INTERNAL_AUTH_SECRET", "a-cluster-secret-with-at-least-32-bytes",
                "PANGROVA_COORDINATOR_ID", "e2e-coordinator");
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                YamlConfigLoader.load(productionTemplate("coordinator.yml"), environment), environment);
        assertThat(config.getString(CoordinatorOptions.INTERNAL_AUTH_SECRET))
                .isEqualTo(environment.get("PANGROVA_INTERNAL_AUTH_SECRET"));
        assertThat(config.getString(CoordinatorOptions.COORDINATOR_ID)).isEqualTo("e2e-coordinator");
        assertThat(config.getString(CoordinatorOptions.METADATA_JDBC_URL))
                .isEqualTo("jdbc:postgresql://postgres:5432/pangrova_meta");
        assertThat(config.getString(CoordinatorOptions.METADATA_JDBC_USERNAME))
                .isEqualTo(environment.get("PANGROVA_DB_USERNAME"));
        assertThat(config.getString(CoordinatorOptions.METADATA_JDBC_PASSWORD))
                .isEqualTo(environment.get("PANGROVA_DB_PASSWORD"));
        assertThat(config.getBoolean(CoordinatorOptions.INDEXING_ENABLED)).isFalse();
        assertThat(config.getString(CoordinatorOptions.INDEX_BASE_PATH))
                .isEqualTo("/var/lib/pangrova/indexes");

        Map<String, String> sharedRootEnvironment = new java.util.HashMap<>(environment);
        sharedRootEnvironment.put("PANGROVA_INDEX_BASE_PATH", "s3a://bucket/pangrova/indexes");
        var sharedRootConfig = PangrovaConfiguration.fromYamlWithEnvOverride(
                YamlConfigLoader.load(productionTemplate("coordinator.yml"), sharedRootEnvironment),
                sharedRootEnvironment);
        assertThat(sharedRootConfig.getString(CoordinatorOptions.INDEX_BASE_PATH))
                .isEqualTo("s3a://bucket/pangrova/indexes");
    }

    @Test
    void productionTemplateDoesNotSilentlyDiscardAMissingDatabasePassword() {
        var environment = Map.of("PANGROVA_DB_HOST", "postgres",
                "PANGROVA_DB_USERNAME", "metadata_reader",
                "PANGROVA_INTERNAL_AUTH_SECRET", "a-cluster-secret-with-at-least-32-bytes",
                "PANGROVA_COORDINATOR_ID", "e2e-coordinator");
        assertThatThrownBy(() -> YamlConfigLoader.load(productionTemplate("coordinator.yml"), environment))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("PANGROVA_DB_PASSWORD");
    }

    @Test
    void productionWorkerTemplateConsumesClusterEnvironmentWithoutFallingBackToDefaultIdentity() throws Exception {
        var environment = Map.of("PANGROVA_WORKER_ID", "e2e-worker-2",
                "PANGROVA_COORDINATOR_URL", "http://coordinator:8080",
                "PANGROVA_ADVERTISED_HOST", "worker-2",
                "PANGROVA_INTERNAL_AUTH_SECRET", "a-cluster-secret-with-at-least-32-bytes");
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                YamlConfigLoader.load(productionTemplate("worker.yml"), environment), environment);
        assertThat(config.getString(WorkerOptions.WORKER_ID)).isEqualTo("e2e-worker-2");
        assertThat(config.getString(WorkerOptions.COORDINATOR_URL)).isEqualTo("http://coordinator:8080");
        assertThat(config.getString(WorkerOptions.WORKER_ADVERTISED_HOST)).isEqualTo("worker-2");
        assertThat(config.getString(WorkerOptions.INTERNAL_AUTH_SECRET))
                .isEqualTo(environment.get("PANGROVA_INTERNAL_AUTH_SECRET"));
        assertThat(config.getBoolean(WorkerOptions.CACHE_ENABLED)).isTrue();
        assertThat(config.getString(WorkerOptions.CACHE_ARTIFACT_PATH))
                .isEqualTo("/mnt/nvme0/pangrova-artifacts");
        assertThat(config.getLong(WorkerOptions.CACHE_ARTIFACT_BYTES)).isPositive();
        assertThat(config.getLong(WorkerOptions.CACHE_ARTIFACT_INDEX_MEMORY_BYTES)).isPositive();
    }

    private static Path productionTemplate(String name) {
        Path directory = Path.of("").toAbsolutePath();
        while (directory != null && !Files.isDirectory(directory.resolve(".mvn"))) {
            directory = directory.getParent();
        }
        if (directory == null) throw new IllegalStateException("Repository root with production templates not found");
        return directory.resolve("etc").resolve(name);
    }
}
