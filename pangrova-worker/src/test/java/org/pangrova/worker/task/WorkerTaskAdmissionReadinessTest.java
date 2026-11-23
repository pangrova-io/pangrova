package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.pangrova.worker.shuffle.NativeShuffleProducerRegistration;
import org.pangrova.worker.task.memory.MemoryRefs;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises actual Jetty submissions, JNI creation and task-owned Shuffle registration. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerTaskAdmissionReadinessTest {
    @Test
    void overlappingExactSubmissionsGetTheSameActualProducerEndpoint() throws Exception {
        var registrationEntered = new CountDownLatch(1);
        var allowRegistration = new CountDownLatch(1);
        var registrations = new AtomicInteger();
        try (var runtime = new ActualNativeTaskFixture()) {
            var shuffle = new NativeShuffleProducerRegistration((handle, command) -> {
                if (isProducerRegistration(command)) {
                    registrations.incrementAndGet();
                    registrationEntered.countDown();
                    awaitRegistrationPermit(allowRegistration);
                }
                return runtime.executor().executeCommand(handle, command);
            }, "127.0.0.1", "");
            var transports = new TransportRefs(shuffle, runtime.transports().resultTransport(),
                    runtime.transports().exchangeTransport());
            var memory = new MemoryRefs(
                    new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager()),
                    runtime.nodePool(), null);
            try (var tasks = new ActualTaskManagerFixture(runtime, "http-ack-ready",
                    new RecordingTaskStatusReporter(), memory, transports)) {
                var config = new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", runtime.resultPort())
                        .withBindHost("127.0.0.1");
                var server = new WorkerHttpServer(config, tasks.manager(), transports.exchangeTransport());
                server.start();
                try {
                    var plan = ActualNativeTaskFixture.valuesTaskRequest(false).getSubstraitPlan().toByteArray();
                    var task = runtime.bindWorker(ActualNativeShuffleRequests.producer(2, plan));
                    var submit = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task"))
                            .timeout(Duration.ofSeconds(15))
                            .header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(SubmitTaskRequest.from(task).encode())).build();
                    var client = HttpClient.newHttpClient();
                    var first = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                    assertThat(registrationEntered.await(10, TimeUnit.SECONDS)).isTrue();
                    var duplicate = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                    try {
                        await().during(Duration.ofMillis(750)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                            assertThat(first.isDone()).as("initial submission waits for its actual endpoint").isFalse();
                            if (duplicate.isDone()) {
                                var early = duplicate.join();
                                var ack = TaskSubmitAck.parseFrom(early.body());
                                throw new AssertionError("Duplicate returned HTTP " + early.statusCode()
                                        + " before producer registration. Shuffle endpoint present: "
                                        + ack.getAdmitted().hasShuffleEndpoint());
                            }
                        });
                    } finally {
                        allowRegistration.countDown();
                    }
                    var firstAck = requireAccepted(first.get(10, TimeUnit.SECONDS));
                    var duplicateAck = requireAccepted(duplicate.get(10, TimeUnit.SECONDS));
                    assertThat(firstAck.getAdmitted().getTaskId()).isEqualTo(task.getAttempt().getTaskId());
                    assertThat(firstAck.getAdmitted().hasShuffleEndpoint()).isTrue();
                    assertThat(firstAck.getAdmitted().getShuffleEndpoint().getPartitionTicketsCount()).isEqualTo(2);
                    assertThat(duplicateAck).isEqualTo(firstAck);
                    assertThat(registrations).hasValue(1);
                    var info = tasks.admittedTask(task);
                    tasks.awaitState(info, org.pangrova.worker.enums.TaskState.FINISHED);
                    assertThat(info.executorHandle()).isZero();
                    assertThat(info.allocatedBufferMemory()).isZero();
                } finally {
                    allowRegistration.countDown();
                    server.stop();
                }
            }
        }
    }

    private static boolean isProducerRegistration(byte[] command) {
        try {
            return RuntimeCommand.parseFrom(command).getCommandType()
                    == RuntimeCommandType.RUNTIME_COMMAND_TYPE_START_SHUFFLE_SERVER;
        } catch (com.google.protobuf.InvalidProtocolBufferException error) {
            throw new AssertionError("Expected a canonical Native command", error);
        }
    }

    private static void awaitRegistrationPermit(CountDownLatch permit) {
        try {
            if (!permit.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Producer registration did not receive permission");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Producer registration interrupted", failure);
        }
    }

    private static TaskSubmitAck requireAccepted(HttpResponse<byte[]> response) throws Exception {
        assertThat(response.statusCode()).isEqualTo(200);
        var ack = TaskSubmitAck.parseFrom(response.body());
        assertThat(ack.hasAdmitted()).isTrue();
        return ack;
    }
}
