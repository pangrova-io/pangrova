package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient;
import org.pangrova.worker.bootstrap.phase.WorkerTelemetryInitializer;

import java.net.InetSocketAddress;
import java.nio.channels.Pipe;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual deregistration HTTP and local owner lifetime; not task or distributed E2E evidence. */
class WorkerBootstrapDeregistrationTest {
    @Test
    void startupRollbackDeregistersAcknowledgedRegistrationBeforeClosingDependencies() throws Exception {
        var registered = new AtomicBoolean();
        var deletes = new AtomicInteger();
        var withdrawal = new java.util.concurrent.atomic.AtomicReference<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/workers/", exchange -> {
            String received = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            boolean registration = exchange.getRequestMethod().equals("POST");
            registered.set(registration);
            if (!registration) {
                deletes.incrementAndGet();
                withdrawal.set(received);
            }
            String status = registration ? "registered" : "deregistered";
            byte[] body = ("{\"status\":\"" + status + "\",\"coordinator_epoch\":1}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        try (var client = new WorkerRegistrationClient(
                "http://127.0.0.1:" + server.getAddress().getPort())) {
            String inventory = org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec.encode(
                    org.pangrova.protocol.v2.CapabilityInventory.parseFrom(
                            org.pangrova.testing.testkit.function.ActualNativeCatalogue.bytes()));
            var request = new org.pangrova.protocol.rest.worker.WorkerRegistrationRequest(
                    "startup-rollback", "127.0.0.1", 0, 0, 0, 2,
                    null, java.util.List.of(), null, inventory,
                    new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                            "00000000-0000-0000-0000-000000000001",
                            "00000000-0000-0000-0000-000000000002"));
            client.register(request).requireCoordinatorEpoch();
            assertThat(registered).isTrue();
            var refs = new WorkerComponentRefs();
            refs.setRegistrationClient(client);
            refs.retainNativeFunctionResource(() -> assertThat(registered).isFalse());

            new WorkerBootstrapCleanup(refs).close(
                    WorkerBootstrapConfig.withDefaults("startup-rollback"),
                    new WorkerTelemetryInitializer());

            assertThat(deletes).hasValue(1);
            assertThat(org.pangrova.common.json.JsonMapperFactory.shared().readValue(withdrawal.get(),
                    org.pangrova.protocol.execution.identity.WorkerExecutionIdentity.class))
                    .isEqualTo(request.workerExecutionIdentity());
            assertThat(registered).isFalse();
            assertThat(refs.registrationClient()).isNull();
        } finally { server.stop(0); }
    }

    @Test
    void failedOwnerCleanupKeepsRegistrationUntilRetryClosesOwner() throws Exception {
        var blocked = new AtomicBoolean(true);
        var deletes = new AtomicInteger();
        var premature = new AtomicBoolean();
        var pipe = Pipe.open();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/workers/", exchange -> {
            if (ackRegistration(exchange)) return;
            deletes.incrementAndGet();
            if (pipe.source().isOpen()) premature.set(true);
            byte[] body = "{\"status\":\"deregistered\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        try (var source = pipe.source(); var sink = pipe.sink();
             var client = new WorkerRegistrationClient("http://127.0.0.1:" + server.getAddress().getPort())) {
            var refs = new WorkerComponentRefs();
            refs.setRegistrationClient(client);
            refs.setCdcConsumer(() -> {
                if (blocked.get()) throw new java.io.IOException("Actual owner still retained");
                source.close();
            });
            register(client, "ordered-deregister");
            var config = WorkerBootstrapConfig.withDefaults("ordered-deregister");
            assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config,
                    new WorkerTelemetryInitializer())).hasMessageContaining("CDC consumer");
            assertThat(deletes).hasValue(0);
            assertThat(refs.registrationClient()).isSameAs(client);
            assertThat(source.isOpen()).isTrue();

            blocked.set(false);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerTelemetryInitializer());
            assertThat(source.isOpen()).isFalse();
            assertThat(deletes).hasValue(1);
            assertThat(premature).isFalse();
            assertThat(refs.registrationClient()).isNull();
        } finally { server.stop(0); }
    }

    @Test
    void wrongSuccessBodyCannotReleaseDependentOwnersAndCanRetry() throws Exception {
        var accepted = new AtomicBoolean(false);
        var deletes = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/workers/", exchange -> {
            if (ackRegistration(exchange)) return;
            deletes.incrementAndGet();
            byte[] body = (accepted.get() ? "{\"status\":\"deregistered\"}" : "{\"status\":\"registered\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        var pipe = Pipe.open();
        try (var source = pipe.source(); var sink = pipe.sink();
             var client = new WorkerRegistrationClient("http://127.0.0.1:" + server.getAddress().getPort())) {
            var refs = new WorkerComponentRefs();
            refs.setRegistrationClient(client);
            refs.retainNativeFunctionResource(source);
            register(client, "unacknowledged-deregister");
            var config = WorkerBootstrapConfig.withDefaults("unacknowledged-deregister");
            assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config,
                    new WorkerTelemetryInitializer())).hasMessageContaining("expected 'deregistered'");
            assertThat(refs.registrationClient()).isSameAs(client);
            assertThat(source.isOpen()).isTrue();
            accepted.set(true);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerTelemetryInitializer());
            assertThat(deletes).hasValue(2);
            assertThat(source.isOpen()).isFalse();
            assertThat(refs.registrationClient()).isNull();
        } finally { server.stop(0); }
    }

    @Test
    void interruptedDeregistrationIsNotAcknowledged() throws Exception {
        try (var server = StubCoordinatorServer.start();
             var client = new WorkerRegistrationClient(server.baseUrl())) {
            register(client, "interrupted-worker");
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(() -> client.deregister("interrupted-worker"))
                        .hasMessageContaining("deregistration interrupted");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally { Thread.interrupted(); }
        }
    }
    private static boolean ackRegistration(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
        if (!exchange.getRequestMethod().equals("POST")) return false;
        exchange.getRequestBody().readAllBytes();
        byte[] body = "{\"status\":\"registered\",\"coordinator_epoch\":1}".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        try (var output = exchange.getResponseBody()) { output.write(body); }
        return true;
    }

    private static void register(WorkerRegistrationClient client, String workerId) throws Exception {
        String inventory = org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec.encode(
                org.pangrova.protocol.v2.CapabilityInventory.parseFrom(
                        org.pangrova.testing.testkit.function.ActualNativeCatalogue.bytes()));
        client.register(new org.pangrova.protocol.rest.worker.WorkerRegistrationRequest(
                workerId, "127.0.0.1", 0, 0, 0, 2, null, java.util.List.of(), null, inventory,
                new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                        "00000000-0000-0000-0000-000000000001",
                        "00000000-0000-0000-0000-000000000002"))).requireCoordinatorEpoch();
    }
}
