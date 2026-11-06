package org.pangrova.coordinator.scheduler;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The actual Coordinator dispatcher must never route a private ticket to HTTP. */
class RemoteTaskDispatcherCopyTlsTest {
    @TempDir Path directory;
    private WorkerHttpServer worker;
    private RemoteTaskDispatcher dispatcher;
    private TaskManager manager;

    @BeforeEach
    void setUp() {
        manager = TaskManager.forHttpWiringOnly();
    }

    @AfterEach
    void close() throws Exception {
        if (dispatcher != null) dispatcher.close();
        if (worker != null) worker.stop();
        if (manager != null) manager.close();
    }

    @Test
    void routesPrivateTaskToRegisteredMutualTlsPortForBothDispatchPaths() throws Exception {
        var certs = certificates();
        manager.observeControlEpoch(1);
        worker = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(
                0, "localhost", 0, "test-secret", "grpc",
                new WorkerHttpServer.CopyTaskTls(certs.serverCert(), certs.serverKey(), certs.ca())),
                manager, TestExchangeTransport.INSTANCE);
        worker.start();
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(),
                new NoOpTracingProvider(), "test-secret");
        dispatcher.configureCopyTaskTls(new ShuffleTlsConfig(
                certs.clientCert(), certs.clientKey(), certs.ca(), "localhost"));
        WorkerInfo target = workerInfo(1, worker.getCopyTaskTlsPort());

        // HTTP port 1 is deliberately unreachable. A real Worker pre-Native
        // rejection proves both dispatch paths reached the mTLS listener.
        assertThatThrownBy(() -> dispatcher.dispatch(copyTask(), target))
                .rootCause().hasMessageContaining("configured Native input runtime");
        assertThatThrownBy(() -> dispatcher.dispatchAsync(copyTask(), target)
                .get(5, TimeUnit.SECONDS))
                .rootCause().hasMessageContaining("configured Native input runtime");
    }

    @Test
    void rejectsMissingSecureEndpointBeforeSendingTicket() {
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(),
                new NoOpTracingProvider(), "test-secret");
        assertThatThrownBy(() -> dispatcher.dispatch(copyTask(), workerInfo(1, 0)))
                .rootCause().hasMessageContaining("registered mTLS port");
    }

    @Test
    void unboundCopyInputCannotReachTransportWithoutDurableAuthority() {
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(),
                new NoOpTracingProvider(), "test-secret");
        var split = org.pangrova.protocol.v2.DataSplit.newBuilder().setConnectorId("copy")
                .setSourceId("query-owned-frozen-source").setSplitId("file-one");
        var pending = SubmitTaskRequest.from(CanonicalTaskRequestTestFixture.builder(
                "task-42", "query-7", 0, 0, TaskKind.TASK_KIND_READ, new byte[] {1}).addSplits(split).build());
        assertThatThrownBy(() -> dispatcher.dispatch(pending, workerInfo(1, 0)))
                .rootCause().hasMessageContaining("frozen source admission is not wired");
        assertThatThrownBy(() -> dispatcher.dispatchAsync(pending, workerInfo(1, 0)))
                .hasMessageContaining("frozen source admission is not wired");
    }

    private WorkerInfo workerInfo(int httpPort, int tlsPort) {
        return new WorkerInfo("worker-1", "localhost", httpPort, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                1024L * 1024 * 1024, Instant.now(), 0.0, null, 2,
                null, List.of(), null, TestWorkerInfoBuilder.canonicalInventory(), tlsPort,
                manager.workerExecutionIdentity());
    }

    private SubmitTaskRequest copyTask() {
        var scope = new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", "task-42", "coordinator", "test", 1, 1,
                4_102_444_800_000L);
        var canonical = CanonicalTaskRequestTestFixture.builder("task-42", "query-7", 0, 0,
                        TaskKind.TASK_KIND_WRITE, new byte[] {1})
                .setCopySourceStagingCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId("task-42:grant").setCpuSlots(1)
                        .setNativeMemoryBytes(64L * 1024 * 1024))
                .build();
        var bound = canonical.toBuilder();
        bound.getAttemptBuilder().setWorkerExecutionIdentity(manager.workerExecutionIdentity().toProto());
        return SubmitTaskRequest.from(bound.build(),
                new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope));
    }

    private Certificates certificates() throws Exception {
        Path ca = directory.resolve("ca.pem");
        Path caKey = directory.resolve("ca.key");
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", caKey.toString(), "-out", ca.toString(),
                "-subj", "/CN=Pangrova test CA", "-days", "1");
        Path server = issue("server", ca, caKey, "subjectAltName=DNS:localhost");
        Path client = issue("client", ca, caKey, "extendedKeyUsage=clientAuth");
        return new Certificates(ca, server, directory.resolve("server.key"),
                client, directory.resolve("client.key"));
    }

    private Path issue(String name, Path ca, Path caKey, String extension) throws Exception {
        Path key = directory.resolve(name + ".key");
        Path csr = directory.resolve(name + ".csr");
        Path cert = directory.resolve(name + ".pem");
        run("openssl", "genpkey", "-algorithm", "RSA", "-out", key.toString(),
                "-pkeyopt", "rsa_keygen_bits:2048");
        run("openssl", "req", "-new", "-key", key.toString(), "-out", csr.toString(),
                "-subj", "/CN=" + name);
        Path ext = directory.resolve(name + ".ext");
        Files.writeString(ext, extension + System.lineSeparator());
        run("openssl", "x509", "-req", "-in", csr.toString(), "-CA", ca.toString(),
                "-CAkey", caKey.toString(), "-CAcreateserial", "-out", cert.toString(),
                "-days", "1", "-extfile", ext.toString());
        return cert;
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(List.of(command)).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        if (process.waitFor() != 0) throw new IllegalStateException(output);
    }

    private record Certificates(Path ca, Path serverCert, Path serverKey,
                                Path clientCert, Path clientKey) {}
}
