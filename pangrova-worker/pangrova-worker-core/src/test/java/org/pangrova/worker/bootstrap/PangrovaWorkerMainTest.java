package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;

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
    void loggingSanitizationPreservesTheActualConfiguredFilename(@TempDir Path directory)
            throws IOException {
        Path configuration = directory.resolve("worker\r\nextra.yml");
        Files.writeString(configuration, "worker:\n  id: actual-configured-worker\n");
        var loaded = PangrovaWorkerMain.loadPangrovaConfiguration(
                new String[]{"--config", configuration.toString()});
        assertThat(PangrovaWorkerMain.buildBootstrapConfig(loaded).workerId())
                .isEqualTo("actual-configured-worker");
    }

    @Test
    void loadsExactWorkerWideChannelLimitsAndRejectsInvalidAdmissionConfiguration() {
        var config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.shuffle.client.max-owned-channels", "17",
                "worker.shuffle.client.max-owned-channels-per-endpoint", "3"));
        var limits = PangrovaWorkerMain.buildBootstrapConfig(config).resourceConfig().shuffleChannelLimits();
        assertThat(limits.maxOwnedChannels()).isEqualTo(17);
        assertThat(limits.maxOwnedChannelsPerEndpoint()).isEqualTo(3);
        for (var invalid : new String[][]{{"0", "1"}, {"1", "0"}, {"1", "2"}}) {
            var rejected = PangrovaConfiguration.fromYaml(Map.of(
                    "worker.shuffle.client.max-owned-channels", invalid[0],
                    "worker.shuffle.client.max-owned-channels-per-endpoint", invalid[1]));
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> PangrovaWorkerMain.buildBootstrapConfig(rejected))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

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
    void buildBootstrapConfigSeparatesShuffleRouteFromControlAndResultRoute() {
        var configuration = PangrovaConfiguration.fromYaml(Map.of(
                "worker.advertised-host", "host.testcontainers.internal",
                "worker.shuffle.advertised-host", "worker-1",
                "worker.transport.tls.enabled", "true",
                "worker.transport.tls.certificate-chain-path", "/secrets/worker.crt",
                "worker.transport.tls.private-key-path", "/secrets/worker.key",
                "worker.transport.tls.root-ca-path", "/secrets/ca.crt",
                "worker.transport.tls.identity", "host.testcontainers.internal"));

        var bootstrap = PangrovaWorkerMain.buildBootstrapConfig(configuration);
        assertThat(bootstrap.advertisedHost()).isEqualTo("host.testcontainers.internal");
        assertThat(bootstrap.shuffleAdvertisedHost()).isEqualTo("worker-1");
        assertThat(bootstrap.transportSecurityConfig().tlsIdentity())
                .isEqualTo("host.testcontainers.internal");

        var inherited = PangrovaWorkerMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("worker.advertised-host", "shared-worker.internal")));
        assertThat(inherited.shuffleAdvertisedHost()).isEqualTo("shared-worker.internal");
    }

    @Test
    void buildBootstrapConfig_defaultsEnableObservabilityAndCdcButNotSecurity() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        WorkerBootstrapConfig result = PangrovaWorkerMain.buildBootstrapConfig(config);

        assertThat(result.observabilityEnabled()).isTrue();
        assertThat(result.cdcEnabled()).isTrue();
        assertThat(result.transportSecurityConfig().enabled()).isFalse();
    }

    @Test
    void buildBootstrapConfigWiresCompleteMutualTlsConfiguration() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.transport.tls.enabled", "true",
                "worker.transport.tls.certificate-chain-path", "/secrets/worker.crt",
                "worker.transport.tls.private-key-path", "/secrets/worker.key",
                "worker.transport.tls.root-ca-path", "/secrets/ca.crt",
                "worker.transport.tls.identity", "worker-1.internal",
                "worker.copy.task-tls-port", "8443"));

        WorkerBootstrapConfig.TransportSecurityConfig security =
                PangrovaWorkerMain.buildBootstrapConfig(config).transportSecurityConfig();

        assertThat(security.enabled()).isTrue();
        assertThat(security.certificateChainPath()).isEqualTo("/secrets/worker.crt");
        assertThat(security.privateKeyPath()).isEqualTo("/secrets/worker.key");
        assertThat(security.rootCaPath()).isEqualTo("/secrets/ca.crt");
        assertThat(security.tlsIdentity()).isEqualTo("worker-1.internal");
        assertThat(security.copyTaskTlsPort()).isEqualTo(8443);
    }

    @Test
    void buildBootstrapConfigRejectsPartialTransportTlsConfiguration() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of(
                "worker.transport.tls.enabled", "true",
                "worker.transport.tls.certificate-chain-path", "/secrets/worker.crt"));

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> PangrovaWorkerMain.buildBootstrapConfig(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires certificate, private key, root CA and identity");
    }

    @Test
    void copyTaskPortRequiresTransportTls() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(
                Map.of("worker.copy.task-tls-port", "8443"));
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> PangrovaWorkerMain.buildBootstrapConfig(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COPY task TLS port requires transport TLS");
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
    void buildBootstrapConfigWiresFailClosedAiAuthorization() {
        PangrovaConfiguration defaults = PangrovaConfiguration.fromYaml(Map.of());
        WorkerBootstrapConfig denied = PangrovaWorkerMain.buildBootstrapConfig(defaults);

        Map<String, String> deniedAiConfig =
                denied.nativeFunctionConfig().configForPlugin("ai-inference");
        assertThat(deniedAiConfig).containsEntry("authorization.mode", "deny");
        assertThat(deniedAiConfig.keySet()).noneMatch(key -> key.startsWith("models."));
        assertThat(deniedAiConfig.get("file.storage-backend")).isNullOrEmpty();

        PangrovaConfiguration enabled = PangrovaConfiguration.fromYaml(Map.of(
                "worker.ai.authorization.mode", "authenticated",
                "worker.ai.authorization.allowed-models", "model-a",
                "worker.ai.authorization.allowed-capabilities", "inference:model-a",
                "worker.ai.authorization.granted-permissions", "AI_MODEL_USAGE"));
        WorkerBootstrapConfig configured = PangrovaWorkerMain.buildBootstrapConfig(enabled);

        assertThat(configured.nativeFunctionConfig().configForPlugin("ai-inference"))
                .containsEntry("authorization.mode", "authenticated")
                .containsEntry("authorization.allowed-models", "model-a");
    }

    @Test
    void defaultYamlFunctionBootstrapUsesServiceLoaderWithoutRequiringFileStorage() {
        WorkerBootstrapConfig defaults = PangrovaWorkerMain.buildBootstrapConfig(
                PangrovaConfiguration.fromYaml(Map.of()));
        WorkerComponentRefs refs = new WorkerComponentRefs();

        org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap.initialize(
                refs, defaults.nativeFunctionConfig());

        assertThat(refs.nativeFunctionBridge()).isNotNull();
        assertThat(refs.nativeFunctionBridge().scalarCount()).isZero();
        org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap.close(refs);
    }

    @Test
    void buildBootstrapConfigPassesDynamicAiModelDescriptorsWithoutSecrets() {
        PangrovaConfiguration source = PangrovaConfiguration.fromYaml(Map.of(
                "worker.ai.models.chat.provider", "openai-compatible",
                "worker.ai.models.chat.provider-model", "chat-deployment-v1",
                "worker.ai.models.chat.version", "2026-08-01",
                "worker.ai.models.chat.endpoint", "https://models.example/v1/chat/completions",
                "worker.ai.models.chat.secret-reference", "env://MODEL_KEY",
                "worker.ai.models.chat.capabilities", "inference,multimodal"));

        Map<String, String> pluginConfig = PangrovaWorkerMain.buildBootstrapConfig(source)
                .nativeFunctionConfig()
                .configForPlugin("ai-inference");

        assertThat(pluginConfig)
                .containsEntry("models.chat.provider", "openai-compatible")
                .containsEntry("models.chat.version", "2026-08-01")
                .containsEntry("models.chat.secret-reference", "env://MODEL_KEY");
    }

    @Test
    void buildBootstrapConfigWiresDistributedFileStorageWithoutSecrets() {
        PangrovaConfiguration source = PangrovaConfiguration.fromYaml(Map.ofEntries(
                Map.entry("worker.ai.file.storage-backend", "s3"),
                Map.entry("worker.ai.file.storage-staging-root", "/var/tmp/files"),
                Map.entry("worker.ai.file.capability-secret-reference", "env://FILE_KEY"),
                Map.entry("worker.ai.file.storage.s3.bucket", "governed-files"),
                Map.entry("worker.ai.file.storage.s3.region", "us-east-1"),
                Map.entry("worker.ai.file.storage.s3.sse", "AES256"),
                Map.entry("worker.ai.file.url.max-connections", "512")));

        Map<String, String> pluginConfig = PangrovaWorkerMain.buildBootstrapConfig(source)
                .nativeFunctionConfig().configForPlugin("ai-inference");

        assertThat(pluginConfig)
                .containsEntry("file.storage-backend", "s3")
                .containsEntry("file.storage-staging-root", "/var/tmp/files")
                .containsEntry("file.capability-secret-reference", "env://FILE_KEY")
                .containsEntry("file.storage.s3.bucket", "governed-files")
                .containsEntry("file.storage.s3.region", "us-east-1")
                .containsEntry("file.storage.s3.sse", "AES256")
                .containsEntry("file.url.max-connections", "512")
                .doesNotContainKey("file.storage-root");
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
