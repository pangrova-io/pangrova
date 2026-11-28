package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.pangrova.worker.shuffle.NativeShuffleProducerRegistrar;
import org.pangrova.worker.task.memory.MemoryRefs;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/** Checks the actual HTTP and JNI setup boundary before a producer endpoint exists. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerSetupFailureAcknowledgementTest {
    private static final String SETUP_FAILURE = "Producer endpoint setup failed in the command fixture";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedNativeProducerSetupRejectsTheOriginalAndReplayWithTheActualCause(boolean linkageError)
            throws Exception {
        var registrations = new AtomicInteger();
        try (var runtime = new ActualNativeTaskFixture()) {
            var shuffle = new NativeShuffleProducerRegistrar((handle, command) -> {
                if (isRegistration(command)) {
                    registrations.incrementAndGet();
                    if (linkageError) throw new NoClassDefFoundError(SETUP_FAILURE);
                    throw new IllegalStateException(SETUP_FAILURE);
                }
                return runtime.executor().executeCommand(handle, command);
            }, "127.0.0.1", "");
            var transports = new TransportRefs(shuffle, runtime.transports().resultTransport(),
                    runtime.transports().exchangeTransport());
            var memory = new MemoryRefs(
                    new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager()),
                    runtime.nodePool(), null);
            try (var tasks = new ActualTaskManagerFixture(runtime, "actual-setup-failure",
                    new RecordingTaskStatusReporter(), memory, transports)) {
                var config = new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", runtime.resultPort())
                        .withBindHost("127.0.0.1");
                var server = new WorkerHttpServer(config, tasks.manager(), transports.exchangeTransport());
                server.start();
                try {
                    var plan = ActualNativeTaskFixture.valuesTaskRequest(false).getSubstraitPlan().toByteArray();
                    var task = runtime.bindWorker(ActualNativeShuffleRequests.producer(2, plan));
                    var submit = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task"))
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(SubmitTaskRequest.from(task).encode()))
                            .build();
                    var client = HttpClient.newHttpClient();
                    var first = client.send(submit, HttpResponse.BodyHandlers.ofByteArray());
                    var ack = TaskSubmitAck.parseFrom(first.body());
                    var info = tasks.admittedTask(task);
                    tasks.awaitState(info, org.pangrova.worker.enums.TaskState.FAILED);
                    assertThat(info.executorHandle()).isZero();
                    assertThat(info.allocatedBufferMemory()).isZero();
                    assertThat(first.statusCode())
                            .as("setup failure response admitted=%s shuffleEndpoint=%s",
                                    ack.hasAdmitted(), ack.getAdmitted().hasShuffleEndpoint())
                            .isEqualTo(500);
                    assertThat(ack.hasRejected()).isTrue();
                    assertThat(ack.getRejected().getReason()).contains(SETUP_FAILURE);

                    var replay = client.send(submit, HttpResponse.BodyHandlers.ofByteArray());
                    assertThat(replay.statusCode()).isEqualTo(first.statusCode());
                    assertThat(replay.body()).isEqualTo(first.body());
                    assertThat(registrations).hasValue(1);
                } finally {
                    server.stop();
                }
            }
        }
    }

    private static boolean isRegistration(byte[] command) {
        try {
            return RuntimeCommand.parseFrom(command).getCommandType()
                    == RuntimeCommandType.RUNTIME_COMMAND_TYPE_START_SHUFFLE_SERVER;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError("Expected canonical Native command", failure);
        }
    }
}
