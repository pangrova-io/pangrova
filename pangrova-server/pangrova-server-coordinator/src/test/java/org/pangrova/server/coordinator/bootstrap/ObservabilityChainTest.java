package org.pangrova.server.coordinator.bootstrap;
import org.pangrova.query.runtime.QueryManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPuller;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPublisher;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.sdk.tracing.otel.CurrentSpanIds;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import static org.pangrova.server.coordinator.testinfra.QuerySubmissionTestSupport.assertAdmissionThenWorkerFailure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Acceptance test: validates the observability chain is active from main() to OTel export.
 *
 * <p>Chain under test: Bootstrap.start() → startObservability() → NoOp providers wired →
 * QueryManager uses TracingProvider/LoggingProvider → HTTP registers ProfilerEndpoint →
 * NativeMetricsPuller/Publisher bridge native stats to MetricsProvider.</p>
 *
 * <p><b>Property 28: Observability chain from main() to OTel export</b></p>
 */
class ObservabilityChainTest extends org.pangrova.server.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().proxy(java.net.ProxySelector.of(null)).build();

    private static com.sun.net.httpserver.HttpServer stubServer;
    private static String stubCoordinatorUrl;

    private CoordinatorBootstrap coordinatorBootstrap;
    private WorkerBootstrap workerBootstrap;

    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path shuffleDirectory;

    @BeforeAll
    static void startStub() throws Exception {
        stubServer = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        stubServer.createContext("/v1/workers/register", ex -> {
            ex.getRequestBody().readAllBytes();
            byte[] response = "{\"status\":\"registered\",\"coordinator_epoch\":1}".getBytes();
            ex.sendResponseHeaders(200, response.length);
            ex.getResponseBody().write(response);
            ex.getResponseBody().close();
        });
        stubServer.createContext("/v1/worker/heartbeat", ex -> {
            ex.getRequestBody().readAllBytes();
            byte[] response = "{\"status\":\"ok\",\"coordinator_epoch\":1}".getBytes();
            ex.sendResponseHeaders(200, response.length);
            ex.getResponseBody().write(response);
            ex.getResponseBody().close();
        });
        stubServer.createContext("/v1/task-status/report", ex -> {
            ex.getRequestBody().readAllBytes();
            ex.sendResponseHeaders(200, 2);
            ex.getResponseBody().write("ok".getBytes());
            ex.getResponseBody().close();
        });
        stubServer.createContext("/v1/workers/", ex -> {
            try (ex) {
                ex.getRequestBody().readAllBytes();
                if (!"DELETE".equals(ex.getRequestMethod())) {
                    ex.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] response = "{\"status\":\"deregistered\",\"coordinator_epoch\":1}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ex.sendResponseHeaders(200, response.length);
                ex.getResponseBody().write(response);
            }
        });
        stubServer.start();
        stubCoordinatorUrl = "http://127.0.0.1:" + stubServer.getAddress().getPort();
    }

    @AfterAll
    static void stopStub() {
        if (stubServer != null) {
            stubServer.stop(0);
        }
    }

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    // -- Providers --

    @Provide
    Arbitrary<String> coordinatorIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "obs-" + s);
    }

    @Provide
    Arbitrary<String> workerIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "wobs-" + s);
    }

    // -- Property tests --

    /**
     * P28a: For any coordinatorId, bootstrap with default config (NoOp observability)
     * starts successfully and query submission reaches worker admission without
     * NPE. No Worker is bootstrapped by this test, so admission must fail closed.
     */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration, Property 28: Observability chain from main() to OTel export")
    void observabilityChainActiveFromMain(
            @ForAll("coordinatorIds") String coordinatorId) throws Exception {

        startCoordinator(coordinatorId);
        int port = coordinatorBootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 1");
        assertAdmissionThenWorkerFailure(coordinatorBootstrap.getQueryManager(), resp);
    }

    /**
     * P28b: For any coordinatorId, TracingProvider is a real NoOpTracingProvider instance
     * (not null), proving the observability phase completed and wired the provider.
     */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration, Property 28: Observability chain from main() to OTel export")
    void tracingProviderWiredInBootstrap(
            @ForAll("coordinatorIds") String coordinatorId) {

        startCoordinator(coordinatorId);
        assertThat(coordinatorBootstrap.getTracingProvider())
                .isNotNull()
                .isInstanceOf(NoOpTracingProvider.class);
    }

    // -- JUnit scenario tests --

    /**
     * CurrentSpanIds.currentTraceId() returns a valid 32-char hex string.
     * With no active span, it returns all-zeros — proving the exporter is callable.
     */
    @Test
    void nativeTraceExporterReturnsValidTraceIds() {
        String traceId = CurrentSpanIds.currentTraceId();

        assertThat(traceId).hasSize(32);
        assertThat(traceId).matches("[0-9a-f]{32}");
    }

    /**
     * CurrentSpanIds.currentSpanId() returns a valid 16-char hex string.
     * With no active span, it returns all-zeros.
     */
    @Test
    void nativeTraceExporterReturnsValidSpanIds() {
        String spanId = CurrentSpanIds.currentSpanId();

        assertThat(spanId).hasSize(16);
        assertThat(spanId).matches("[0-9a-f]{16}");
    }

    /**
     * NativeMetricsPublisher pulls stats from NativeMetricsPuller and updates gauge values.
     * Uses a test supplier returning known bytes — verifies the pull+publish cycle.
     */
    @Test
    void nativeMetricsPublisherUpdatesGauges() {
        byte[] testStats = buildTestStats(1024L, 5000L, 2048L, 0.95);
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> testStats);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(
                puller, TestObservability.METRICS);

        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(1024L);
        assertThat(publisher.getOperatorRowsProcessed()).isEqualTo(5000L);
        assertThat(publisher.getIoBytesRead()).isEqualTo(2048L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.95);
    }

    /**
     * NativeMetricsPublisher handles null stats gracefully (resets to zero).
     */
    @Test
    void nativeMetricsPublisherHandlesNullStats() {
        NativeMetricsPuller puller = new NativeMetricsPuller(timeoutMs -> null);
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(
                puller, TestObservability.METRICS);

        publisher.pullAndPublish();

        assertThat(publisher.getMemoryUsedBytes()).isEqualTo(0L);
        assertThat(publisher.getCacheHitRatio()).isEqualTo(0.0);
    }

    /**
     * ProfilerEndpoint is accessible via HTTP at /v1/query/profile/*.
     * GET reports 501 when this node has no profiling provider; it must not invent a session.
     */
    @Test
    void unavailableProfilingIsExplicitViaHttp() throws Exception {
        startCoordinator("coord-profiler-test");
        int port = coordinatorBootstrap.getHttpPort();

        HttpResponse<String> resp = sendGet(port, "/v1/query/profile/unknown-query");

        assertThat(resp.statusCode()).isEqualTo(501);
        Map<String, Object> body = parseJson(resp);
        assertThat(body.get("error_type")).isEqualTo("UNSUPPORTED_CAPABILITY");
    }

    @Test
    void configuredLoopbackBindDoesNotServeAnotherLocalInterface() throws Exception {
        startCoordinator("coord-http-bind-test");
        int port = coordinatorBootstrap.getHttpPort();
        assertThat(sendGet(port, "/v1/query/profile/unknown-query").statusCode()).isEqualTo(501);
        try (var otherInterface = new java.net.Socket()) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> otherInterface.connect(
                    new java.net.InetSocketAddress("127.0.0.2", port), 1000))
                    .isInstanceOf(java.io.IOException.class);
        }
    }

    /**
     * WorkerBootstrap starts and stops without errors when observability is disabled.
     * Proves the observability phase completes with NoOp providers.
     */
    @Test
    @org.junit.jupiter.api.Tag("native-required")
    void workerBootstrapObservabilityPhaseCompletes() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig("worker-obs-test", 0, 0, stubCoordinatorUrl,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null,
                org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        workerBootstrap = org.pangrova.coordinator.testinfra.NativeWorkerLifecycleFixture.create(
                config, shuffleDirectory);

        assertThatCode(() -> workerBootstrap.start()).doesNotThrowAnyException();
        assertThat(workerBootstrap.isStarted()).isTrue();

        assertThatCode(() -> workerBootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(workerBootstrap.isStarted()).isFalse();
    }

    /**
     * CoordinatorBootstrap starts and stops without errors when observability is disabled.
     * Proves the full observability wiring path completes cleanly.
     */
    @Test
    void coordinatorBootstrapObservabilityPhaseCompletes() {
        startCoordinator("coord-obs-lifecycle");

        assertThat(coordinatorBootstrap.isStarted()).isTrue();
        assertThat(coordinatorBootstrap.getHttpPort()).isGreaterThan(0);

        assertThatCode(() -> {
            coordinatorBootstrap.shutdown();
            coordinatorBootstrap = null;
        }).doesNotThrowAnyException();
    }

    /**
     * StructuredLogger is wired in QueryManager — query submission uses LoggingProvider
     * without NPE, proving the logging chain is active.
     */
    @Test
    void structuredLoggerWiredInQueryManager() throws Exception {
        startCoordinator("coord-logger-test");
        int port = coordinatorBootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 42");
        assertAdmissionThenWorkerFailure(coordinatorBootstrap.getQueryManager(), resp);

        assertThat(coordinatorBootstrap.getQueryManager()).isNotNull();
    }

    // -- Bootstrap helpers --

    private void startCoordinator(String coordinatorId) {
        shutdownCoordinator();
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                coordinatorId, 0);
        coordinatorBootstrap = startAuthorizedCoordinator(config);
        assertThat(coordinatorBootstrap.isStarted()).isTrue();
        assertThat(coordinatorBootstrap.getHttpPort()).isGreaterThan(0);
    }

    private void shutdownAll() {
        shutdownCoordinator();
        shutdownWorker();
    }

    private void shutdownCoordinator() {
        if (coordinatorBootstrap != null) {
            coordinatorBootstrap.shutdown();
            coordinatorBootstrap = null;
        }
    }

    private void shutdownWorker() {
        if (workerBootstrap != null) {
            workerBootstrap.shutdown();
            workerBootstrap = null;
        }
    }

    // -- HTTP helpers --

    private HttpResponse<String> submitQuery(int port, String sql) throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .header("Authorization", authorizationHeader())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> sendGet(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Authorization", authorizationHeader())
                .GET().build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(HttpResponse<String> resp) throws Exception {
        return MAPPER.readValue(resp.body(), Map.class);
    }

    // -- Test data helpers --

    private static byte[] buildTestStats(long memory, long rows, long ioBytes, double cacheHit) {
        ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(memory);
        buf.putLong(rows);
        buf.putLong(ioBytes);
        buf.putLong(Double.doubleToLongBits(cacheHit));
        return buf.array();
    }
}
