package org.pangrova.testing.integration.e2e.coordination;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.pangrova.common.http.jetty.EmbeddedHttpServerFactory;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.coordination.api.membership.WorkerRegistrationIdentity;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.coordinator.http.endpoint.WorkerResource;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcWorkerRegistry;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.security.core.authentication.AuthenticationManager;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.function.ActualNativeCatalogue;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.WorkerHeartbeatBootstrap;
import org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient;
import org.pangrova.worker.task.TaskManager;

import static org.assertj.core.api.Assertions.*;

/** Real production heartbeat wiring -> authenticated HTTP -> PostgreSQL identity conditions. */
class WorkerHeartbeatIdentityIntegrationTest extends ContainerTestBase {
    @Test
    void oldClientCannotRepairOverReplacementAndProductionHeartbeatPermanentlyFencesIt() throws Exception {
        var registry = new JdbcWorkerRegistry(MigratedPostgres.create());
        var workers = new WorkerManager(20_000, 60_000, registry);
        String secret = "heartbeat-integration-internal-secret";
        try (var authentication = new AuthenticationManager(List.of());
             var tasks = TaskManager.forHttpWiringOnly()) {
            var failInitialAcknowledgement = new java.util.concurrent.atomic.AtomicBoolean(true);
            var config = new ResourceConfig().register((jakarta.ws.rs.container.ContainerResponseFilter) (request, response) -> {
                        if (request.getUriInfo().getPath().equals("v1/workers/register")
                                && failInitialAcknowledgement.compareAndSet(true, false)) {
                            // Registration committed, but the client receives failure instead of its acknowledgement.
                            response.setStatus(503);
                            response.setEntity(java.util.Map.of("status", "error", "message", "acknowledgement fault"));
                        }
                    }).register(new WorkerResource(workers))
                    .register(JacksonFeature.class).register(new ProductionMapperResolver())
                    .register(new AuthenticationFilter(authentication, secret));
            var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            handler.setContextPath("/");
            handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
            var server = EmbeddedHttpServerFactory.create(0, handler);
            var refs = new WorkerComponentRefs();
            refs.setTaskManager(tasks);
            try {
                server.start();
                String url = "http://127.0.0.1:" + EmbeddedHttpServerFactory.getPort(server);
                try (var oldClient = new WorkerRegistrationClient(url, secret);
                     var newClient = new WorkerRegistrationClient(url, secret)) {
                    var old = registration(tasks.workerExecutionIdentity());
                    var replacement = registration(new WorkerExecutionIdentity(
                            old.workerExecutionIdentity().admissionDomainId(), java.util.UUID.randomUUID().toString()));
                    assertThatThrownBy(() -> oldClient.register(old)).hasMessageContaining("503");
                    assertThat(registry.getWorker(old.workerId()).orElseThrow().incarnationId())
                            .isEqualTo(old.workerExecutionIdentity().incarnationId());
                    oldClient.register(old).requireCoordinatorEpoch();
                    var expected = new WorkerRegistrationIdentity(old.workerId(),
                            old.workerExecutionIdentity().admissionDomainId(), old.workerExecutionIdentity().incarnationId());
                    assertThat(registry.unregister(expected)).isTrue();
                    // The second call uses the actual /refresh endpoint and repairs a missing row.
                    oldClient.register(old).requireCoordinatorEpoch();
                    assertThat(registry.getWorker(old.workerId()).orElseThrow().registrationIdentity()).isEqualTo(expected);
                    newClient.register(replacement).requireCoordinatorEpoch();
                    var before = registry.getWorker(old.workerId());
                    assertThatThrownBy(() -> oldClient.register(old)).hasMessageContaining("409");
                    var client = HttpClient.newHttpClient();
                    assertThat(post(client, url, secret, "{\"worker_id\":\"same-worker\"}").statusCode()).isEqualTo(400);
                    var mapper = JsonMapperFactory.shared();
                    var heartbeat = new HeartbeatRequest(old.workerId(), "HIGH", 0.9, 99, 1L, old.workerExecutionIdentity());
                    assertThat(post(client, url, "wrong-secret", mapper.writeValueAsString(heartbeat)).statusCode()).isEqualTo(401);
                    var rejected = post(client, url, secret, mapper.writeValueAsString(heartbeat));
                    assertThat(rejected.statusCode()).isEqualTo(409);
                    assertThat(mapper.readTree(rejected.body()).path("action").asText()).isEqualTo("fenced");
                    assertThat(registry.getWorker(old.workerId())).isEqualTo(before);
                    refs.setRegistrationClient(oldClient);
                    var bootstrap = new WorkerBootstrapConfig(old.workerId(), 0, 0, url, 25,
                            false, null, null, 300, false, null, null, secret, null, null,
                            "192.0.2.10", null, null, null,
                            org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
                    WorkerHeartbeatBootstrap.startHeartbeat(bootstrap, refs);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10))
                            .until(() -> refs.heartbeatSender().isFenced());
                    assertThat(registry.getWorker(old.workerId())).isEqualTo(before);
                    WorkerHeartbeatBootstrap.stopHeartbeat(refs);
                    assertThat(workers.getWorker(old.workerId()).workerExecutionIdentity())
                            .isEqualTo(replacement.workerExecutionIdentity());
                    newClient.deregister(replacement.workerId());
                }
            } finally {
                WorkerHeartbeatBootstrap.stopHeartbeat(refs);
                server.stop();
            }
        }
    }

    private static HttpResponse<String> post(HttpClient client, String url, String secret, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url + "/v1/worker/heartbeat"))
                .header("Content-Type", "application/json").header("Authorization", "Bearer " + secret)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static WorkerRegistrationRequest registration(WorkerExecutionIdentity identity) throws Exception {
        String inventory = CapabilityInventoryCodec.encode(CapabilityInventory.parseFrom(ActualNativeCatalogue.bytes()));
        return new WorkerRegistrationRequest("same-worker", "192.0.2.10", 8080, 9090, 9191, 2,
                null, List.of(), null, inventory, identity);
    }
    /** Match CoordinatorHttpServer's JSON configuration, including existing optional native metrics. */
    public static final class ProductionMapperResolver
            implements jakarta.ws.rs.ext.ContextResolver<com.fasterxml.jackson.databind.ObjectMapper> {
        private final com.fasterxml.jackson.databind.ObjectMapper mapper = JsonMapperFactory.create();
        @Override
        public com.fasterxml.jackson.databind.ObjectMapper getContext(Class<?> type) { return mapper; }
    }

}
