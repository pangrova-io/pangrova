package org.pangrova.testing.integration.e2e.coordination;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.pangrova.common.http.jetty.EmbeddedHttpServerFactory;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.coordinator.http.endpoint.WorkerResource;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcWorkerRegistry;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.security.core.authentication.AuthenticationManager;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.function.ActualNativeCatalogue;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Worker client -> authenticated Coordinator HTTP -> exact PostgreSQL withdrawal. */
class WorkerWithdrawalIdentityIntegrationTest extends ContainerTestBase {
    @Test
    void delayedShutdownOfOldProcessCannotWithdrawItsReplacement() throws Exception {
        var registry = new JdbcWorkerRegistry(MigratedPostgres.create());
        var workers = new WorkerManager(20_000, 60_000, registry);
        String secret = "withdrawal-integration-internal-secret";
        try (var authentication = new AuthenticationManager(List.of())) {
            var config = new ResourceConfig().register(new WorkerResource(workers))
                    .register(JacksonFeature.class)
                    .register(new AuthenticationFilter(authentication, secret));
            var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            handler.setContextPath("/");
            handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
            var server = EmbeddedHttpServerFactory.create(0, handler);
            try {
                server.start();
                String url = "http://127.0.0.1:" + EmbeddedHttpServerFactory.getPort(server);
                try (var oldClient = new WorkerRegistrationClient(url, secret);
                     var newClient = new WorkerRegistrationClient(url, secret)) {
                    var old = registration("00000000-0000-0000-0000-0000000000e1");
                    var replacement = registration("00000000-0000-0000-0000-0000000000e2");
                    assertThatThrownBy(() -> oldClient.deregister(old.workerId()))
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("No matching");
                    oldClient.register(old).requireCoordinatorEpoch();
                    newClient.register(replacement).requireCoordinatorEpoch();
                    assertThatThrownBy(() -> oldClient.register(replacement))
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot change");
                    oldClient.deregister(old.workerId());
                    assertThat(registry.getWorker(old.workerId()).orElseThrow().incarnationId())
                            .isEqualTo(replacement.workerExecutionIdentity().incarnationId());
                    assertThat(workers.getWorker(old.workerId()).workerExecutionIdentity())
                            .isEqualTo(replacement.workerExecutionIdentity());
                    newClient.deregister(replacement.workerId());
                    assertThat(registry.getWorker(old.workerId())).isEmpty();
                    assertThat(workers.getWorker(old.workerId())).isNull();
                    oldClient.deregister(old.workerId());
                    newClient.deregister(replacement.workerId());
                    assertThat(registry.getWorker(old.workerId())).isEmpty();
                }
            } finally {
                server.stop();
            }
        }
    }

    private static WorkerRegistrationRequest registration(String incarnation) throws Exception {
        String inventory = CapabilityInventoryCodec.encode(CapabilityInventory.parseFrom(ActualNativeCatalogue.bytes()));
        // This registration/withdrawal test never dispatches to the advertised Worker endpoint.
        // Use a numeric documentation address; the actual Coordinator HTTP connection is loopback.
        return new WorkerRegistrationRequest("same-worker", "192.0.2.10", 8080, 9090, 9191, 2,
                null, List.of(), null, inventory, new WorkerExecutionIdentity(
                        "00000000-0000-0000-0000-0000000000d1", incarnation));
    }
}
