package org.pangrova.coordinator.testinfra;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Proves this component fixture starts the packaged Native runtime and releases registration. */
class NativeWorkerLifecycleFixtureTest {
    @TempDir
    Path shuffleDirectory;

    @Test
    @Tag("native-required")
    void startsNativeWithExplicitBudgetAndDeregistersOnShutdown() throws Exception {
        var deleted = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/workers/register", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                byte[] result = "{\"status\":\"registered\",\"coordinator_epoch\":1}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, result.length);
                exchange.getResponseBody().write(result);
            }
        });
        server.createContext("/v1/workers/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                if (!"DELETE".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                deleted.incrementAndGet();
                byte[] result = "{\"status\":\"deregistered\",\"coordinator_epoch\":1}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, result.length);
                exchange.getResponseBody().write(result);
            }
        });
        server.start();
        var config = config("http://127.0.0.1:" + server.getAddress().getPort());
        var worker = NativeWorkerLifecycleFixture.create(config, shuffleDirectory);
        try {
            worker.start();
            assertThat(worker.isStarted()).isTrue();
            assertThat(worker.getTransportPort()).isPositive();
            assertThat(worker.getTaskManager()).isNotNull();
        } finally {
            try {
                worker.shutdown();
            } finally {
                server.stop(0);
            }
        }
        assertThat(worker.isStarted()).isFalse();
        assertThat(deleted.get()).isEqualTo(1);
    }

    @Test
    void rejectsMissingConfigurationOrSpillDirectoryBeforeStartup() {
        var config = config("http://localhost:1");
        assertThatThrownBy(() -> NativeWorkerLifecycleFixture.create(null, shuffleDirectory))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> NativeWorkerLifecycleFixture.create(config, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static WorkerBootstrapConfig config(String coordinatorUrl) {
        return new WorkerBootstrapConfig("native-lifecycle", 0, 0, coordinatorUrl,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS, false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1),
                null, null, null, null, null, null, null, null,
                org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
    }
}
