package org.pangrova.server.coordinator.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.constants.CoordinatorOptions;

import java.util.Map;

class CoordinatorIndexStorageConfigurationTest {
    @Test
    void enabledIndexingUsesTheConfiguredDurableRoot() {
        var flags = new CoordinatorBootstrapConfig.FeatureFlags(
                false, false, true, false, false, false, false, false,
                false, false, true);
        var config = new CoordinatorBootstrapConfig(
                CoordinatorBootstrapConfig.withDefaults("coordinator", 8080).core(),
                flags,
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null, "s3a://bucket/pangrova/indexes", null,
                new org.pangrova.coordinator.generation.DurableGenerationDispatcher.Settings(
                        java.time.Duration.ofMinutes(2), java.time.Duration.ofSeconds(5), 32),
                org.pangrova.coordinator.generation.DurableGenerationPublisher.defaultSettings());

        assertThat(config.baseIndexPath()).isEqualTo("s3a://bucket/pangrova/indexes");
        assertThat(CoordinatorIndexingConfiguration.from(PangrovaConfiguration.fromYaml(Map.of(
                        CoordinatorOptions.INDEX_BASE_PATH.key(), "s3a://bucket/pangrova/indexes")),
                config.baseIndexPath()).baseIndexPath()).isEqualTo("s3a://bucket/pangrova/indexes");
    }

    @Test
    void enabledIndexingRejectsAMissingDurableRoot() {
        var flags = new CoordinatorBootstrapConfig.FeatureFlags(
                false, false, true, false, false, false, false, false,
                false, false, true);
        var config = new CoordinatorBootstrapConfig(
                CoordinatorBootstrapConfig.withDefaults("coordinator", 8080).core(),
                flags,
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null, null, null,
                new org.pangrova.coordinator.generation.DurableGenerationDispatcher.Settings(
                        java.time.Duration.ofMinutes(2), java.time.Duration.ofSeconds(5), 32),
                org.pangrova.coordinator.generation.DurableGenerationPublisher.defaultSettings());

        assertThatThrownBy(config::baseIndexPath)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("indexing.base-path");
    }

    @Test
    void lifecycleSettingsComeFromImmutableDeploymentConfiguration() {
        var deploymentConfig = PangrovaConfiguration.fromYaml(Map.of(
                "indexing.build.parallelism", "7",
                "indexing.gc.interval-minutes", "11",
                "indexing.consistency.check-interval-minutes", "13",
                "indexing.consistency.auto-rebuild-enabled", "true",
                "indexing.artifact.dispatch-lease", "3m",
                "indexing.artifact.dispatch-max-in-flight", "7",
                "indexing.artifact.publish-batch-size", "9"));

        var indexing = CoordinatorIndexingConfiguration.from(deploymentConfig, "/var/lib/pangrova/indexes");
        var bootstrap = PangrovaCoordinatorMain.buildBootstrapConfig(deploymentConfig);

        assertThat(indexing.buildParallelism()).isEqualTo(7);
        assertThat(indexing.gcIntervalMinutes()).isEqualTo(11);
        assertThat(indexing.lifecycle().consistency().checkIntervalMinutes()).isEqualTo(13);
        assertThat(indexing.lifecycle().consistency().autoRebuildEnabled()).isTrue();
        assertThat(CoordinatorIndexingConfiguration.buildTimeout(indexing))
                .isEqualTo(java.time.Duration.ofMinutes(26));
        assertThat(bootstrap.artifactDispatchLease()).isEqualTo(java.time.Duration.ofMinutes(3));
        assertThat(bootstrap.artifactDispatchMaxInFlight()).isEqualTo(7);
        assertThat(bootstrap.artifactPublicationSettings().batchSize()).isEqualTo(9);
    }

    @Test
    void enabledIndexingUsesTheEffectiveSystemPropertyRoot() {
        String previous = System.getProperty("pangrova.index.basePath");
        System.setProperty("pangrova.index.basePath", "s3a://bucket/fallback/indexes");
        try {
            var deploymentConfig = PangrovaConfiguration.fromYaml(Map.of(
                    CoordinatorOptions.INDEXING_ENABLED.key(), "true",
                    CoordinatorOptions.INDEX_BASE_PATH.key(), ""));

            var bootstrap = PangrovaCoordinatorMain.buildBootstrapConfig(deploymentConfig);

            assertThat(bootstrap.indexingEnabled()).isTrue();
            assertThat(bootstrap.baseIndexPath()).isEqualTo("s3a://bucket/fallback/indexes");
            assertThat(bootstrap.indexingConfig()).isNotNull();
        } finally {
            if (previous == null) {
                System.clearProperty("pangrova.index.basePath");
            } else {
                System.setProperty("pangrova.index.basePath", previous);
            }
        }
    }

    @Test
    void automaticMaintenanceDefaultsOffUntilLifecycleQualification() {
        var indexing = CoordinatorIndexingConfiguration.from(
                PangrovaConfiguration.fromYaml(Map.of()), "/var/lib/pangrova/indexes");

        assertThat(indexing.lifecycle().consistency().autoRebuildEnabled()).isFalse();
        assertThat(indexing.lifecycle().commitListener().enabled()).isFalse();
    }
}
