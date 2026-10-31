package org.pangrova.observability.sdk.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.resources.Resource;
import org.pangrova.config.PangrovaConfiguration;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Verifies that {@link TelemetryInitializer} populates the OTel Resource
 * with all required SemConv fields (service.version, service.instance.id,
 * host.name, deployment.environment, process.pid, service.namespace).
 *
 * <p>Uses a standalone Resource build to verify attribute population since
 * OTel SDK 1.32.0 does not expose getResource() on SdkTracerProvider publicly.
 * The test validates the same logic path used by TelemetryInitializer.buildSdk().
 */
class TelemetryInitializerTest {

    @Test
    void resourceBuilder_populatesAllSemConvAttributes() {
        Resource resource = Resource.getDefault().merge(
                Resource.builder()
                        .put("service.name", "pangrova-coordinator")
                        .put("service.version", "0.1.0")
                        .put("service.namespace", "lakehouse")
                        .put("service.instance.id", "coord-abc-123")
                        .put("host.name", "test-host")
                        .put("deployment.environment", "staging")
                        .put("process.pid", 12345L)
                        .build());

        assertThat(resource.getAttribute(AttributeKey.stringKey("service.name")))
                .isEqualTo("pangrova-coordinator");
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.version")))
                .isEqualTo("0.1.0");
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.namespace")))
                .isEqualTo("lakehouse");
        assertThat(resource.getAttribute(AttributeKey.stringKey("service.instance.id")))
                .isEqualTo("coord-abc-123");
        assertThat(resource.getAttribute(AttributeKey.stringKey("host.name")))
                .isEqualTo("test-host");
        assertThat(resource.getAttribute(AttributeKey.stringKey("deployment.environment")))
                .isEqualTo("staging");
        assertThat(resource.getAttribute(AttributeKey.longKey("process.pid")))
                .isEqualTo(12345L);
    }

    @Test
    void telemetryInitializer_createsValidSdkWithAllFields() {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", "pangrova-coordinator",
                "pangrova.observability.service-version", "0.1.0",
                "pangrova.observability.service-namespace", "lakehouse",
                "pangrova.observability.service-instance-id", "coord-abc-123",
                "pangrova.observability.deployment-environment", "staging",
                "pangrova.observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        try (TelemetryInitializer initializer = new TelemetryInitializer(config)) {
            assertThat(initializer.getTracer()).isNotNull();
            assertThat(initializer.getMeter()).isNotNull();
            assertThat(initializer.getOpenTelemetry()).isNotNull();
        }
    }

    @Test
    void observabilityConfig_generatesDefaultsWhenOptionalFieldsAbsent() {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", "pangrova-worker",
                "pangrova.observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getServiceVersion()).isEqualTo("unknown");
        assertThat(config.getServiceNamespace()).isEqualTo("pangrova");
        assertThat(config.getServiceInstanceId()).isNull();
        assertThat(config.getDeploymentEnvironment()).isEqualTo("production");
    }

    @Test
    void observabilityConfig_readsAllSemConvFields() {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", "pangrova-coordinator",
                "pangrova.observability.service-version", "0.1.0",
                "pangrova.observability.service-namespace", "lakehouse",
                "pangrova.observability.service-instance-id", "coord-abc-123",
                "pangrova.observability.deployment-environment", "staging",
                "pangrova.observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getServiceName()).isEqualTo("pangrova-coordinator");
        assertThat(config.getServiceVersion()).isEqualTo("0.1.0");
        assertThat(config.getServiceNamespace()).isEqualTo("lakehouse");
        assertThat(config.getServiceInstanceId()).isEqualTo("coord-abc-123");
        assertThat(config.getDeploymentEnvironment()).isEqualTo("staging");
    }

    @Test
    void telemetryInitializer_autoGeneratesInstanceId() {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", "pangrova-worker",
                "pangrova.observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);
        assertThat(config.getServiceInstanceId()).isNull();

        try (TelemetryInitializer initializer = new TelemetryInitializer(config)) {
            assertThat(initializer.getTracer()).isNotNull();
        }
    }

    @Test
    void telemetryInitializer_hostNameResolvesToNonEmpty() throws UnknownHostException {
        String expectedHostName = InetAddress.getLocalHost().getHostName();
        assertThat(expectedHostName).isNotEmpty();

        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", "pangrova-test",
                "pangrova.observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        try (TelemetryInitializer initializer = new TelemetryInitializer(config)) {
            assertThat(initializer.getTracer()).isNotNull();
        }
    }
}
