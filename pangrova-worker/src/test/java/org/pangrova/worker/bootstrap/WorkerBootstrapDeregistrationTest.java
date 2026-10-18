package org.pangrova.worker.bootstrap;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.worker.bootstrap.lifecycle.WorkerRegistrationClient;
import org.pangrova.worker.bootstrap.phase.WorkerObservabilitySetup;

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
    void failedOwnerCleanupKeepsRegistrationUntilRetryClosesOwner() throws Exception {
        var blocked = new AtomicBoolean(true);
        var deletes = new AtomicInteger();
        var premature = new AtomicBoolean();
        var pipe = Pipe.open();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/workers/", exchange -> {
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
            var config = WorkerBootstrapConfig.withDefaults("ordered-deregister");
            assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config,
                    new WorkerObservabilitySetup(), true)).hasMessageContaining("CDC consumer");
            assertThat(deletes).hasValue(0);
            assertThat(refs.registrationClient()).isSameAs(client);
            assertThat(source.isOpen()).isTrue();

            blocked.set(false);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerObservabilitySetup(), true);
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
            var config = WorkerBootstrapConfig.withDefaults("unacknowledged-deregister");
            assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config,
                    new WorkerObservabilitySetup(), true)).hasMessageContaining("expected 'deregistered'");
            assertThat(refs.registrationClient()).isSameAs(client);
            assertThat(source.isOpen()).isTrue();
            accepted.set(true);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerObservabilitySetup(), true);
            assertThat(deletes).hasValue(2);
            assertThat(source.isOpen()).isFalse();
            assertThat(refs.registrationClient()).isNull();
        } finally { server.stop(0); }
    }

    @Test
    void interruptedDeregistrationIsNotAcknowledged() throws Exception {
        try (var server = StubCoordinatorServer.start();
             var client = new WorkerRegistrationClient(server.baseUrl())) {
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(() -> client.deregister("interrupted-worker"))
                        .hasMessageContaining("deregistration interrupted");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally { Thread.interrupted(); }
        }
    }
}
