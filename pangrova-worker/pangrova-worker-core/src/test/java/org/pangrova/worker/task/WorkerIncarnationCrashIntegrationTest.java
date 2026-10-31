package org.pangrova.worker.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.nativebridge.jni.NativeFunctionConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real Native owner in a killed JVM, followed by exact HTTP recovery in a separate replacement JVM. */
@Tag("native-required")
class WorkerIncarnationCrashIntegrationTest {
    @TempDir Path directory;

    @Test
    void killedNativeOwnerIsProvenRetiredByReplacementWorkerOverHttp() throws Exception {
        Process first = start("first", 0);
        Process replacement = null;
        try {
            String[] original = ready(first, "first");
            TaskRequest issued = TaskRequest.parseFrom(Base64.getDecoder().decode(original[1]));
            assertThat(Long.parseLong(original[2])).isPositive();
            first.destroyForcibly();
            assertThat(first.waitFor(15, TimeUnit.SECONDS)).isTrue();
            replacement = start("replacement", Integer.parseInt(original[0]));
            String[] current = ready(replacement, "replacement");
            assertThat(current[0]).isEqualTo(original[0]);
            assertThat(replacement.pid()).isNotEqualTo(first.pid());
            var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            var seal = CanonicalTaskAdmissionSeals.create(issued);
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + current[0]
                            + "/v1/task/" + issued.getAttempt().getTaskId()))
                    .header("Content-Type", "application/x-protobuf")
                    .timeout(Duration.ofSeconds(10))
                    .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(seal.toByteArray())).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            assertThat(response.statusCode()).isEqualTo(200);
            var receipt = TaskReport.parseFrom(response.body());
            assertThat(CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(receipt).getAttempt())
                    .isEqualTo(issued.getAttempt());
            assertThat(receipt.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_INCARNATION_RETIRED);
            var unknown = seal.toBuilder().setAttempt(seal.getAttempt().toBuilder().setWorkerExecutionIdentity(
                    seal.getAttempt().getWorkerExecutionIdentity().toBuilder()
                            .setIncarnationId("00000000-0000-0000-0000-000000000099"))).build();
            var refused = client.send(HttpRequest.newBuilder(request.uri())
                    .header("Content-Type", "application/x-protobuf").timeout(Duration.ofSeconds(10))
                    .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(unknown.toByteArray())).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(refused.statusCode()).isEqualTo(409);
            var staleTarget = issued.toBuilder().setAttempt(issued.getAttempt().toBuilder().setControlEpoch(2)).build();
            byte[] lateBody = org.pangrova.common.json.provider.jackson.JsonMapperFactory.shared().writeValueAsBytes(
                    org.pangrova.protocol.rest.task.SubmitTaskRequest.from(staleTarget));
            var late = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + original[0] + "/v1/task"))
                    .header("Content-Type", "application/json").timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(lateBody)).build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(late.statusCode()).isEqualTo(409);
        } finally {
            stop(first);
            if (replacement != null) stop(replacement);
        }
    }

    private Process start(String role, int port) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"), NativeOwnerProcess.class.getName(),
                directory.toString(), role, Integer.toString(port)).redirectErrorStream(true)
                .redirectOutput(directory.resolve(role + ".log").toFile()).start();
    }

    private String[] ready(Process process, String role) throws Exception {
        Path ready = directory.resolve(role + ".ready");
        await().atMost(Duration.ofSeconds(45)).until(() -> Files.exists(ready) || !process.isAlive());
        assertThat(process.isAlive()).withFailMessage(() -> diagnostic(role)).isTrue();
        return Files.readString(ready).split("\\n");
    }

    private String diagnostic(String role) {
        try {
            return Files.readString(directory.resolve(role + ".log"));
        } catch (java.io.IOException failure) {
            return failure.toString();
        }
    }

    private static void stop(Process process) throws Exception {
        if (process.isAlive()) process.destroyForcibly();
        assertThat(process.waitFor(15, TimeUnit.SECONDS)).isTrue();
    }

    public static final class NativeOwnerProcess {
        private NativeOwnerProcess() {}
        public static void main(String[] args) throws Exception {
            Path directory = Path.of(args[0]);
            boolean original = args[1].equals("first");
            try (var runtime = new ActualNativeTaskFixture(NativeFunctionConfig.DISABLED, directory.resolve("state"));
                 var fixture = new ActualTaskManagerFixture(runtime, "crash-worker")) {
                var manager = fixture.manager();
                manager.requireControlEpochHandshake();
                manager.observeControlEpoch(original ? 1 : 2);
                var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(
                        Integer.parseInt(args[2]), "127.0.0.1", runtime.resultPort()), manager,
                        runtime.transports().exchangeTransport(), null, null, runtime.resultTickets());
                server.start();
                try {
                    String task = "none";
                    long handle = 0;
                    if (original) {
                        var request = runtime.valuesRequest(true);
                        var info = fixture.submit(request);
                        await().atMost(Duration.ofSeconds(15)).until(() -> info.executorHandle() > 0);
                        handle = info.executorHandle();
                        assertThat(runtime.drainEvidence(handle, request).getDrained()).isFalse();
                        task = Base64.getEncoder().encodeToString(request.toByteArray());
                    }
                    Path pending = directory.resolve(args[1] + ".pending");
                    Files.writeString(pending, server.getPort() + "\n" + task + "\n" + handle);
                    Files.move(pending, directory.resolve(args[1] + ".ready"),
                            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                    new java.util.concurrent.CountDownLatch(1).await();
                } finally {
                    server.stop();
                }
            }
        }
    }
}
