package org.pangrova.server.worker.embedding;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks embedded worker configuration before any server is started. */
class EmbeddedWorkerTest {

    @Test
    void configuredConnectorsReachEmbeddedAndIndependentWorkerSettings(@org.junit.jupiter.api.io.TempDir java.nio.file.Path root) {
        var settings = PangrovaConfiguration.fromYaml(Map.of("connector.plugin-directory", root.toString()));
        assertThat(EmbeddedWorker.buildConfig(settings, 18080).pluginDirectory()).contains(root);
    }

    @Test
    void standalonePreservesConfiguredShuffleCacheFunctionAndCopyTransport() {
        var settings = PangrovaConfiguration.fromYaml(Map.of(
                "worker.shuffle.mode", "REMOTE",
                "worker.native.cache.enabled", "true",
                "worker.native.cache.artifact-path", "/cache/artifacts",
                "worker.ai.authorization.allowed-models", "model-a",
                "worker.transport.tls.enabled", "true",
                "worker.transport.tls.certificate-chain-path", "worker.crt",
                "worker.transport.tls.private-key-path", "worker.key",
                "worker.transport.tls.root-ca-path", "ca.crt",
                "worker.transport.tls.identity", "worker.example",
                "worker.copy.task-tls-port", "8443"));

        var config = EmbeddedWorker.buildConfig(settings, 18080);
        assertThat(config.shuffleMode()).isEqualTo(org.pangrova.config.enums.ShuffleMode.REMOTE);
        assertThat(config.cacheConfig().enabled()).isTrue();
        assertThat(config.nativeFunctionConfig().configForPlugin("ai-inference"))
                .containsEntry("authorization.allowed-models", "model-a");
        assertThat(config.transportSecurityConfig().copyTaskTlsPort()).isEqualTo(8443);
    }

    @Test
    void workerUsesTheLocalCoordinatorPortWhenNoUrlIsConfigured() {
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                Map.of("worker.id", "standalone-worker"));

        var worker = EmbeddedWorker.buildConfig(config, 18080);

        assertThat(worker.coordinatorUrl()).isEqualTo("http://localhost:18080");
    }

    @Test
    void explicitCoordinatorUrlTakesPrecedenceOverTheLocalPort() {
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                Map.of("worker.id", "standalone-worker",
                        "worker.coordinator-url", "https://coordinator.example.test:8443"));

        var worker = EmbeddedWorker.buildConfig(config, 18080);

        assertThat(worker.coordinatorUrl()).isEqualTo("https://coordinator.example.test:8443");
    }
}
