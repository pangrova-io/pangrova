package org.pangrova.observability.sdk.config;

import org.pangrova.observability.sdk.enums.SamplingStrategy;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.ConfigurationException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

/**
 * Initializes the OpenTelemetry SDK from {@link ObservabilityConfig}.
 * Provides pre-configured {@link Tracer} and {@link Meter} instances.
 */
public final class TelemetryInitializer implements AutoCloseable {

    private static final String INSTRUMENTATION_NAME = "org.pangrova";

    private final OpenTelemetrySdk sdk;
    private final Tracer tracer;
    private final Meter meter;

    /**
     * Initializes the OpenTelemetry SDK from the given configuration, failing fast on error.
     */
    public TelemetryInitializer(ObservabilityConfig config) {
        try {
            this.sdk = buildSdk(config);
            this.tracer = sdk.getTracer(INSTRUMENTATION_NAME);
            this.meter = sdk.getMeter(INSTRUMENTATION_NAME);
        } catch (Exception e) {
            throw new ConfigurationException(
                    PangrovaErrorCode.OBS_SDK_INIT_FAILED,
                    "Failed to initialize OpenTelemetry SDK: " + e.getMessage(), e);
        }
    }

    public Tracer getTracer() { return tracer; }

    public Meter getMeter() { return meter; }

    public OpenTelemetry getOpenTelemetry() { return sdk; }

    /**
     * Shuts down the OpenTelemetry SDK, flushing pending telemetry data.
     * Implements {@link AutoCloseable} for try-with-resources support.
     */
    @Override
    public void close() {
        sdk.close();
    }

    private static OpenTelemetrySdk buildSdk(ObservabilityConfig config) {
        String instanceId = config.getServiceInstanceId() != null
                ? config.getServiceInstanceId()
                : UUID.randomUUID().toString();

        Resource resource = Resource.getDefault().merge(
                Resource.builder()
                        .put("service.name", config.getServiceName())
                        .put("service.version", config.getServiceVersion())
                        .put("service.namespace", config.getServiceNamespace())
                        .put("service.instance.id", instanceId)
                        .put("host.name", resolveHostName())
                        .put("deployment.environment", config.getDeploymentEnvironment())
                        .put("process.pid", ProcessHandle.current().pid())
                        .build());

        byte[] trustedCertificates = readTrustedCertificates(config);
        SdkTracerProvider tracerProvider = buildTracerProvider(config, resource, trustedCertificates);
        try {
            SdkMeterProvider meterProvider = buildMeterProvider(config, resource, trustedCertificates);
            try {
                return OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .setMeterProvider(meterProvider)
                        .build();
            } catch (RuntimeException | Error failure) {
                try { meterProvider.close(); } catch (RuntimeException suppressed) { failure.addSuppressed(suppressed); }
                throw failure;
            }
        } catch (RuntimeException | Error failure) {
            try { tracerProvider.close(); } catch (RuntimeException suppressed) { failure.addSuppressed(suppressed); }
            throw failure;
        }
    }

    private static String resolveHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    private static SdkTracerProvider buildTracerProvider(ObservabilityConfig config,
                                                          Resource resource, byte[] trustedCertificates) {
        OtlpGrpcSpanExporterBuilder exporterBuilder = OtlpGrpcSpanExporter.builder()
                .setEndpoint(config.getOtlpEndpoint());
        config.getOtlpHeaders().forEach(exporterBuilder::addHeader);
        if ("gzip".equalsIgnoreCase(config.getOtlpCompression())) exporterBuilder.setCompression("gzip");
        if (trustedCertificates != null) exporterBuilder.setTrustedCertificates(trustedCertificates);
        OtlpGrpcSpanExporter exporter = exporterBuilder.build();

        Sampler sampler = resolveSampler(config);

        return SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .setSampler(sampler)
                .build();
    }

    private static SdkMeterProvider buildMeterProvider(ObservabilityConfig config,
                                                        Resource resource, byte[] trustedCertificates) {
        OtlpGrpcMetricExporterBuilder exporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(config.getOtlpEndpoint());
        config.getOtlpHeaders().forEach(exporterBuilder::addHeader);
        if ("gzip".equalsIgnoreCase(config.getOtlpCompression())) exporterBuilder.setCompression("gzip");
        if (trustedCertificates != null) exporterBuilder.setTrustedCertificates(trustedCertificates);
        OtlpGrpcMetricExporter exporter = exporterBuilder.build();

        PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
                .setInterval(Duration.ofMillis(config.getMetricsIntervalMs()))
                .build();

        return SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(reader)
                .build();
    }

    private static Sampler resolveSampler(ObservabilityConfig config) {
        if (config.getSamplingStrategy() == SamplingStrategy.RATE_LIMITING) {
            Sampler rateLimiter = new TokenBucketSampler(config.getTracesPerSecond());
            return Sampler.parentBased(rateLimiter);
        }
        return Sampler.traceIdRatioBased(config.getSamplingProbability());
    }

    private static byte[] readTrustedCertificates(ObservabilityConfig config) {
        String path = config.getOtlpTlsCertPath();
        if (path == null) return null;
        try {
            return Files.readAllBytes(Path.of(path));
        } catch (java.io.IOException failure) {
            throw new ConfigurationException(PangrovaErrorCode.OBS_SDK_INIT_FAILED,
                    "Failed to read TLS certificate: " + path, failure);
        }
    }

    /**
     * Token-bucket rate-limiting sampler that caps trace creation at a fixed
     * number of traces per second. Tokens refill continuously based on elapsed
     * time since last refill.
     *
     * <p>Thread-safe via synchronized access to the mutable bucket state.</p>
     */
    private static final class TokenBucketSampler implements Sampler {

        /** Nanoseconds in one second, for converting elapsed nanos to seconds. */
        private static final double NANOS_PER_SECOND = 1_000_000_000.0;

        private final double maxTracesPerSecond;
        private final double maxTokens;
        private double availableTokens;
        private long lastRefillNanos;

        TokenBucketSampler(double maxTracesPerSecond) {
            this.maxTracesPerSecond = maxTracesPerSecond;
            this.maxTokens = Math.max(1.0, maxTracesPerSecond);
            this.availableTokens = maxTokens;
            this.lastRefillNanos = System.nanoTime();
        }

        @Override
        public synchronized io.opentelemetry.sdk.trace.samplers.SamplingResult shouldSample(
                io.opentelemetry.context.Context parentContext,
                String traceId,
                String name,
                io.opentelemetry.api.trace.SpanKind spanKind,
                io.opentelemetry.api.common.Attributes attributes,
                java.util.List<io.opentelemetry.sdk.trace.data.LinkData> parentLinks) {
            refill();
            if (availableTokens >= 1.0) {
                availableTokens -= 1.0;
                return io.opentelemetry.sdk.trace.samplers.SamplingResult.recordAndSample();
            }
            return io.opentelemetry.sdk.trace.samplers.SamplingResult.drop();
        }

        @Override
        public String getDescription() {
            return "TokenBucketSampler{maxTracesPerSecond=" + maxTracesPerSecond + "}";
        }

        private void refill() {
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillNanos) / NANOS_PER_SECOND;
            availableTokens = Math.min(maxTokens, availableTokens + elapsedSeconds * maxTracesPerSecond);
            lastRefillNanos = now;
        }
    }
}
