package org.pangrova.coordinator.http.endpoint;

import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.coordinator.http.RequestLimitConfig;
import org.pangrova.security.core.authentication.AuthenticationManager;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.rest.worker.WorkerControlResponse;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies R10.4: WorkerResource rejects workers with incompatible protocol versions.
 */
class WorkerResourceVersionCheckTest {

    private static final String INTERNAL_SECRET = "worker-version-test-internal-secret";
    private WorkerResource resource;
    private WorkerManager workerManager;

    @BeforeEach
    void setUp() {
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        resource = new WorkerResource(workerManager);
    }

    @Test
    void registrationEnvelopeRejectsLegacyVersion1() {
        assertThatThrownBy(() -> new WorkerRegistrationRequest(
                "worker-1", "host-1", 8080, 9090, 0,
                1, null, List.of(), null, canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical v2");
    }

    @Test
    void register_compatibleVersion2_succeeds() throws Exception {
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-2", "host-2", 8080, 9090, 0,
                2, null, List.of(), null, canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-2"));

        HttpResponse<String> response = registerOverHttp(request, INTERNAL_SECRET);

        assertThat(response.statusCode()).isEqualTo(200);
        WorkerControlResponse body = JsonMapperFactory.reader()
                .forType(WorkerControlResponse.class).readValue(response.body());
        assertThat(body.status()).isEqualTo("registered");
        assertThat(body.requireCoordinatorEpoch()).isEqualTo(1L);
    }

    @Test
    void registeredWorkerAcceptsValidInventoryLargerThanGenericHttpLimit() throws Exception {
        var inventory = TestWorkerInfoBuilder.canonicalInventory().toBuilder()
                .addSupportedRelationTypeUrls("x".repeat(1024 * 1024)).build();
        var request = new WorkerRegistrationRequest("worker-large-inventory", "host-2", 8080,
                9090, 0, 2, null, List.of(), null, CapabilityInventoryCodec.encode(inventory),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-large-inventory"));
        assertThat(JsonMapperFactory.writer().writeValueAsBytes(request).length)
                .isGreaterThan(RequestLimitConfig.DEFAULT_MAX_REQUEST_SIZE_BYTES);
        assertThat(registerOverHttp(request, INTERNAL_SECRET).statusCode()).isEqualTo(200);
    }

    @Test
    void registeredWorkerRejectsOversizedMetadataEvenWhenInventoryIsSmall() throws Exception {
        var request = new WorkerRegistrationRequest("worker-large-metadata", "host-2", 8080,
                9090, 0, 2, "x".repeat(WorkerRegistrationRequest.MAX_METADATA_JSON_BYTES),
                List.of(), null, canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-large-metadata"));
        assertThat(registerOverHttp(request, INTERNAL_SECRET).statusCode()).isEqualTo(400);
    }

    @Test
    void compatibleInventoryCannotReplaceInternalServiceAuthentication() throws Exception {
        var request = new WorkerRegistrationRequest(
                "worker-unauthenticated", "host-2", 8080, 9090, 0,
                2, null, List.of(), null, canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-unauthenticated"));
        assertThat(registerOverHttp(request, "wrong-secret").statusCode()).isEqualTo(401);
    }

    private HttpResponse<String> registerOverHttp(
            WorkerRegistrationRequest request, String secret) throws Exception {
        return requestOverHttp("POST", "/v1/workers/register", request, secret);
    }

    private HttpResponse<String> requestOverHttp(String method, String path, Object request, String secret) throws Exception {
        try (var authentication = new AuthenticationManager(List.of())) {
            var config = new ResourceConfig().register(resource).register(JacksonFeature.class)
                    .register(new RequestLimitFilter(RequestLimitConfig.defaults()))
                    .register(new AuthenticationFilter(authentication, INTERNAL_SECRET));
            var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            handler.setContextPath("/");
            handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
            var server = EmbeddedHttpServerFactory.create(0, handler);
            try {
                server.start();
                var httpRequest = HttpRequest.newBuilder(URI.create("http://localhost:"
                        + EmbeddedHttpServerFactory.getPort(server) + path))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + secret)
                        .method(method, HttpRequest.BodyPublishers.ofString(
                                JsonMapperFactory.writer().writeValueAsString(request))).build();
                return HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
            } finally {
                server.stop();
            }
        }
    }

    @Test
    void register_missingCanonicalInventory_isRejected() {
        assertThatThrownBy(() -> new WorkerRegistrationRequest(
                "worker-no-inventory", "host-2", 8080, 9090, 0,
                2, null, List.of(), null, null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-no-inventory")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capabilityInventory");
    }

    @Test
    void register_invalidVersion0_failsBeforeTransport() {
        assertThatThrownBy(() ->
                new WorkerRegistrationRequest(
                        "worker-3", "host-3", 8080, 9090, 0,
                        0, null, List.of(), null, canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocolVersion");
    }

    @Test
    void registrationEnvelopeRejectsFutureVersion() {
        assertThatThrownBy(() -> new WorkerRegistrationRequest(
                "worker-4", "host-4", 8080, 9090, 0,
                99, null, List.of(), null, canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical v2");
    }

    @Test
    void canonicalProtocolVersionAcceptsOnlyV2() {
        assertThat(WorkerResource.isCanonicalProtocolVersion(2)).isTrue();
    }

    @Test
    void nonCanonicalProtocolVersionsAreRejected() {
        assertThat(WorkerResource.isCanonicalProtocolVersion(0)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(-1)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(1)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(3)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(99)).isFalse();
    }

    @Test
    void authenticatedStaleProcessWithdrawalCannotDeleteReplacement() throws Exception {
        var old = TestWorkerInfoBuilder.active("withdrawal", "localhost", 8080, 9090);
        workerManager.registerWorker(old);
        var replacement = TestWorkerInfoBuilder.active(old.workerId(), "localhost", 8181, 9191,
                new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                        old.workerExecutionIdentity().admissionDomainId(),
                        "00000000-0000-0000-0000-0000000000e2"));
        workerManager.registerWorker(replacement);
        String path = "/v1/workers/" + old.workerId();
        assertThat(requestOverHttp("DELETE", path, old.workerExecutionIdentity(), INTERNAL_SECRET).statusCode()).isEqualTo(200);
        assertThat(workerManager.getWorker(old.workerId()).workerExecutionIdentity()).isEqualTo(replacement.workerExecutionIdentity());
        assertThat(requestOverHttp("DELETE", path, null, INTERNAL_SECRET).statusCode()).isEqualTo(400);
        assertThat(requestOverHttp("DELETE", path, replacement.workerExecutionIdentity(), "wrong-secret").statusCode()).isEqualTo(401);
        assertThat(workerManager.getWorker(old.workerId())).isNotNull();
        assertThat(requestOverHttp("DELETE", path, replacement.workerExecutionIdentity(), INTERNAL_SECRET).statusCode()).isEqualTo(200);
        assertThat(workerManager.getWorker(old.workerId())).isNull();
        assertThat(requestOverHttp("DELETE", path, replacement.workerExecutionIdentity(), INTERNAL_SECRET).statusCode()).isEqualTo(200);
    }

    @Test
    void exactWithdrawalAfterCoordinatorFailoverDoesNotRequireALocalCacheEntry() throws Exception {
        var registry = new InMemoryWorkerRegistry();
        var previous = new WorkerManager(20_000, 60_000, registry);
        var worker = TestWorkerInfoBuilder.active("withdrawal-failover", "localhost", 8080, 9090);
        previous.registerWorker(worker);
        workerManager = new WorkerManager(20_000, 60_000, registry);
        resource = new WorkerResource(workerManager);
        assertThat(workerManager.getWorker(worker.workerId())).isNull();
        assertThat(requestOverHttp("DELETE", "/v1/workers/" + worker.workerId(),
                worker.workerExecutionIdentity(), INTERNAL_SECRET).statusCode()).isEqualTo(200);
        assertThat(registry.getWorker(worker.workerId())).isEmpty();
    }

    private static String canonicalInventory() {
        return CapabilityInventoryCodec.encode(TestWorkerInfoBuilder.canonicalInventory());
    }
}
