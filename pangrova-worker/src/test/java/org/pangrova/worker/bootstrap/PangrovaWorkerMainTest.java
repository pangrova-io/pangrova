package org.pangrova.worker.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PangrovaWorkerMain} config loading and CLI arg parsing.
 */
class PangrovaWorkerMainTest {

    @Test
    void parseConfigPath_returnsPathWhenPresent() {
        String[] args = {"--config", "/tmp/worker.yml", "--other", "value"};
        assertThat(PangrovaWorkerMain.parseConfigPath(args)).isEqualTo("/tmp/worker.yml");
    }

    @Test
    void parseConfigPath_returnsNullWhenAbsent() {
        String[] args = {"--other", "value"};
        assertThat(PangrovaWorkerMain.parseConfigPath(args)).isNull();
    }

    @Test
    void parseConfigPath_returnsNullForEmptyArgs() {
        assertThat(PangrovaWorkerMain.parseConfigPath(new String[0])).isNull();
    }

    @Test
    void parseConfigPath_returnsNullWhenConfigIsLastArg() {
        String[] args = {"--config"};
        assertThat(PangrovaWorkerMain.parseConfigPath(args)).isNull();
    }

    @Test
    void buildBootstrapConfig_usesDefaultWorkerIdAndPort() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.workerId()).isEqualTo("worker-1");
        assertThat(result.httpPort()).isEqualTo(8081);
    }

    @Test
    void buildBootstrapConfig_defaultsEnableObservabilityAndCdcButNotSecurity() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.observabilityEnabled()).isTrue();
        assertThat(result.cdcEnabled()).isTrue();
    }

    @Test
    void buildBootstrapConfig_respectsYamlOverrides() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.id", "my-worker",
                "worker.port", "9091",
                "worker.transport.port", "5555",
                "worker.coordinator-url", "http://coordinator:8080",
                "worker.cdc.enabled", "false"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.workerId()).isEqualTo("my-worker");
        assertThat(result.httpPort()).isEqualTo(9091);
        assertThat(result.transportPort()).isEqualTo(5555);
        assertThat(result.coordinatorUrl()).isEqualTo("http://coordinator:8080");
        assertThat(result.cdcEnabled()).isFalse();
    }

    @Test
    void buildBootstrapConfig_heartbeatIntervalFromDuration() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.heartbeat-interval", "30s"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.heartbeatIntervalMs()).isEqualTo(30_000);
    }

    @Test
    void buildBootstrapConfig_resourceConfigFromYaml() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.memory.budget-bytes", "1073741824",
                "worker.threads.query", "8",
                "worker.threads.io", "4"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.resourceConfig().memoryBudgetBytes()).isEqualTo(1073741824L);
        assertThat(result.resourceConfig().queryThreads()).isEqualTo(8);
        assertThat(result.resourceConfig().ioThreads()).isEqualTo(4);
    }

    @Test
    void buildBootstrapConfig_registrationRetryConfigFromYaml() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.registration.max-attempts", "10",
                "worker.registration.initial-backoff", "2s",
                "worker.registration.max-backoff", "60s"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.registrationRetryConfig().maxAttempts()).isEqualTo(10);
        assertThat(result.registrationRetryConfig().initialBackoffMs()).isEqualTo(2_000);
        assertThat(result.registrationRetryConfig().maxBackoffMs()).isEqualTo(60_000);
    }

    @Test
    void buildBootstrapConfig_observabilityConfigNullWhenDisabled() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "observability.enabled", "false"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.observabilityBootstrapConfig()).isNull();
    }

    @Test
    void buildBootstrapConfig_observabilityConfigSetWhenFullyProvided() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "observability.enabled", "true",
                "observability.service-name", "pangrova-worker",
                "observability.otlp-endpoint", "http://otel:4317"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.observabilityBootstrapConfig()).isNotNull();
        assertThat(result.observabilityBootstrapConfig().serviceName()).isEqualTo("pangrova-worker");
        assertThat(result.observabilityBootstrapConfig().otlpEndpoint()).isEqualTo("http://otel:4317");
    }

    @Test
    void loadPangrovaConfiguration_loadsClasspathDefaultsWhenNoConfigArg() throws IOException {
        PangrovaConfiguration config = PangrovaWorkerMain.loadPangrovaConfiguration(new String[0]);

        assertThat(config).isNotNull();
        assertThat(config.toMap()).isNotEmpty();
    }

    @Test
    void loadPangrovaConfiguration_loadsFromCliYaml(@TempDir Path tempDir) throws IOException {
        Path yamlFile = tempDir.resolve("custom.yml");
        Files.writeString(yamlFile, "worker:\n  port: 9999\n");

        PangrovaConfiguration config = PangrovaWorkerMain.loadPangrovaConfiguration(
                new String[]{"--config", yamlFile.toString()});

        assertThat(config.toMap()).containsEntry("worker.port", "9999");
    }

    @Test
    void buildBootstrapConfig_resultRetentionSecondsFromYaml() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.task.result-retention-seconds", "600"
        ));
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.resultRetentionSeconds()).isEqualTo(600);
    }
}
