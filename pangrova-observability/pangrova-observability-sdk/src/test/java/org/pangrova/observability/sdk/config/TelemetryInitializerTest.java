package org.pangrova.observability.sdk.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.pangrova.config.PangrovaConfiguration;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Verifies deployment configuration and spans received by an OTLP/gRPC collector. */
class TelemetryInitializerTest {
    @Test
    void deployedExporterSendsSpanAndResourceToCollector() throws Exception {
        var received = new CompletableFuture<ExportTraceServiceRequest>();
        var collector = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(new TraceServiceGrpc.TraceServiceImplBase() {
                    @Override
                    public void export(ExportTraceServiceRequest request,
                            StreamObserver<ExportTraceServiceResponse> response) {
                        received.complete(request);
                        response.onNext(ExportTraceServiceResponse.getDefaultInstance());
                        response.onCompleted();
                    }
                }).build().start();
        try {
            var deployment = PangrovaConfiguration.fromYaml(Map.of(
                    "observability.service-name", "pangrova-coordinator",
                    "observability.service-version", "test-version",
                    "observability.service-namespace", "lakehouse",
                    "observability.service-instance-id", "coordinator-test",
                    "observability.deployment-environment", "staging",
                    "observability.sampling.probability", "1",
                    "observability.otlp-compression", "gzip",
                    "observability.otlp-endpoint", "http://127.0.0.1:" + collector.getPort()));
            try (var initializer = new TelemetryInitializer(ObservabilityConfig.fromDeployment(deployment).orElseThrow())) {
                initializer.getTracer().spanBuilder("catalog.refresh").startSpan().end();
                var sdk = (OpenTelemetrySdk) initializer.getOpenTelemetry();
                assertThat(sdk.getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS).isSuccess()).isTrue();
                var resourceSpans = received.get(10, TimeUnit.SECONDS).getResourceSpans(0);
                assertThat(resourceSpans.getScopeSpans(0).getSpans(0).getName()).isEqualTo("catalog.refresh");
                var attributes = resourceSpans.getResource().getAttributesList().stream()
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                assertThat(attributes.get("service.name").getStringValue()).isEqualTo("pangrova-coordinator");
                assertThat(attributes.get("service.version").getStringValue()).isEqualTo("test-version");
                assertThat(attributes.get("service.namespace").getStringValue()).isEqualTo("lakehouse");
                assertThat(attributes.get("service.instance.id").getStringValue()).isEqualTo("coordinator-test");
                assertThat(attributes.get("deployment.environment").getStringValue()).isEqualTo("staging");
                assertThat(attributes.get("process.pid").getIntValue()).isEqualTo(ProcessHandle.current().pid());
                assertThat(attributes.get("host.name").getStringValue()).isNotBlank();
            }
        } finally {
            collector.shutdownNow();
            assertThat(collector.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void telemetryInitializer_createsValidSdkWithAllFields() {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "observability.service-name", "pangrova-coordinator",
                "observability.service-version", "0.1.0",
                "observability.service-namespace", "lakehouse",
                "observability.service-instance-id", "coord-abc-123",
                "observability.deployment-environment", "staging",
                "observability.otlp-endpoint", "http://localhost:4317"));

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
                "observability.service-name", "pangrova-worker",
                "observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getServiceVersion()).isEqualTo("unknown");
        assertThat(config.getServiceNamespace()).isEqualTo("pangrova");
        assertThat(config.getServiceInstanceId()).isNull();
        assertThat(config.getDeploymentEnvironment()).isEqualTo("production");
    }

    @Test
    void observabilityConfig_readsAllSemConvFields() {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "observability.service-name", "pangrova-coordinator",
                "observability.service-version", "0.1.0",
                "observability.service-namespace", "lakehouse",
                "observability.service-instance-id", "coord-abc-123",
                "observability.deployment-environment", "staging",
                "observability.otlp-endpoint", "http://localhost:4317"));

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
                "observability.service-name", "pangrova-worker",
                "observability.otlp-endpoint", "http://localhost:4317"));

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
                "observability.service-name", "pangrova-test",
                "observability.otlp-endpoint", "http://localhost:4317"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        try (TelemetryInitializer initializer = new TelemetryInitializer(config)) {
            assertThat(initializer.getTracer()).isNotNull();
        }
    }
}
