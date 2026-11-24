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
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.pangrova.worker.shuffle.NativeShuffleProducerRegistration;
import org.pangrova.worker.task.memory.MemoryRefs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises actual Jetty submissions, JNI creation and task-owned endpoint readiness. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerTaskAdmissionReadinessTest {
    @Test
    void exactRetriesKeepControlResponsiveAndShareOneProducerRegistration() throws Exception {
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
                var healthCheck = new org.pangrova.common.lifecycle.HealthCheckEndpoint(
                        org.pangrova.common.enums.ClusterNodeType.WORKER,
                        new org.pangrova.common.lifecycle.HealthCheckEndpoint.HealthConditions() {
                            @Override public boolean isHttpReady() { return true; }
                            @Override public boolean isTransportReady() { return true; }
                            @Override public boolean isNativeReady() { return true; }
                            @Override public int resultServerPort() { return runtime.resultPort(); }
                            @Override public boolean resultServerRunning() { return true; }
                            @Override public int activeWorkerCount() { return 1; }
                            @Override public int activeCatalogCount() { return 0; }
                        });
                var server = new WorkerHttpServer(config, tasks.manager(), transports.exchangeTransport(), healthCheck);
                server.start();
                try {
                    var plan = ActualNativeTaskFixture.valuesTaskRequest(false).getSubstraitPlan().toByteArray();
                    var task = runtime.bindWorker(ActualNativeShuffleRequests.producer(2, plan));
                    var submit = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task"))
                            .timeout(Duration.ofSeconds(30))
                            .header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(SubmitTaskRequest.from(task).encode())).build();
                    var client = HttpClient.newHttpClient();
                    var first = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                    assertThat(registrationEntered.await(10, TimeUnit.SECONDS)).isTrue();
                    int replayCount = 140;
                    var duplicates = java.util.stream.IntStream.range(0, replayCount)
                            .mapToObj(index -> client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray())).toList();
                    var health = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/health"))
                            .timeout(Duration.ofSeconds(30)).GET().build();
                    var pending = pendingAcknowledgements(server);
                    try {
                        await().atMost(Duration.ofSeconds(8)).until(() -> pending.availablePermits() == 0);
                        await().atMost(Duration.ofSeconds(3)).until(() ->
                                duplicates.stream().filter(java.util.concurrent.CompletableFuture::isDone).count()
                                        == 12);
                        assertThat(first.isDone()).isFalse();
                        var healthResponse = HttpClient.newHttpClient()
                                .sendAsync(health, HttpResponse.BodyHandlers.discarding());
                        assertThat(healthResponse.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
                    } finally {
                        allowRegistration.countDown();
                    }
                    var firstAck = requireAccepted(first.get(10, TimeUnit.SECONDS));
                    int busy = 0;
                    for (var replay : duplicates) {
                        var reply = replay.get(10, TimeUnit.SECONDS);
                        if (reply.statusCode() == 503) {
                            assertThat(TaskSubmitAck.parseFrom(reply.body()).getRejected().getCode())
                                    .isEqualTo("SERVICE_UNAVAILABLE");
                            busy++;
                        } else {
                            assertThat(requireAccepted(reply)).isEqualTo(firstAck);
                        }
                    }
                    assertThat(busy).isEqualTo(12);
                    await().atMost(Duration.ofSeconds(3)).until(() -> pending.availablePermits() == 128);
                    assertThat(firstAck.getAdmitted().getTaskId()).isEqualTo(task.getAttempt().getTaskId());
                    assertThat(firstAck.getAdmitted().hasShuffleEndpoint()).isTrue();
                    assertThat(firstAck.getAdmitted().getShuffleEndpoint().getPartitionTicketsCount()).isEqualTo(2);
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

    @Test
    void originalAndExactReplayWaitForNativeFlightToBecomeReadable() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var start = new CompletableFuture<Runnable>();
            var memory = new MemoryRefs(new ResourceManager(runtime.arbitrator(),
                    new ThreadBudgetTracker(8, 4, 2), new SpillManager()), runtime.nodePool(), null);
            try (var tasks = new ActualTaskManagerFixture(runtime, "http-output-ready", new RecordingTaskStatusReporter(),
                    memory, runtime.transports(), holdResultStart(runtime, start))) {
                var config = new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", runtime.resultPort())
                        .withBindHost("127.0.0.1");
                var server = new WorkerHttpServer(config, tasks.manager(), runtime.transports().exchangeTransport(),
                        null, null, runtime.resultTickets());
                server.start();
                try {
                    var request = runtime.valuesRequest(true);
                    var submit = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task"))
                            .timeout(Duration.ofSeconds(10)).header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(SubmitTaskRequest.from(request).encode())).build();
                    var client = HttpClient.newHttpClient();
                    var original = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                    var allowNativeStart = start.get(5, TimeUnit.SECONDS);
                    var replay = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                    await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                        try (var available = tasks.manager().admitTaskBody()) {
                            available.reserve(SubmitTaskRequest.MAX_HTTP_BODY_BYTES);
                        }
                    });
                    await().during(Duration.ofMillis(900)).atMost(Duration.ofSeconds(3)).until(() ->
                            !original.isDone() && !replay.isDone());
                    allowNativeStart.run();
                    var ack = requireAccepted(original.get(5, TimeUnit.SECONDS));
                    assertThat(ack).isEqualTo(requireAccepted(replay.get(5, TimeUnit.SECONDS)));
                    assertThat(ack.getAdmitted().getTaskId()).isEqualTo(request.getAttempt().getTaskId());
                    var endpoint = ack.getAdmitted().getResultEndpoint();
                    assertThat(endpoint.getPort()).isEqualTo(runtime.resultPort());
                    assertThat(endpoint.getTicket()).isEqualTo(runtime.resultTicket(request));
                    try (var allocator = new org.apache.arrow.memory.RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
                         var flight = org.apache.arrow.flight.FlightClient.builder(allocator,
                                 org.apache.arrow.flight.Location.forGrpcInsecure(endpoint.getHost(), endpoint.getPort())).build();
                         var stream = flight.getStream(new org.apache.arrow.flight.Ticket(
                                 endpoint.getTicket().getBytes(java.nio.charset.StandardCharsets.UTF_8)))) {
                        assertThat(stream.next()).isTrue();
                        assertThat(stream.getRoot().getRowCount()).isEqualTo(1);
                        assertThat(stream.getRoot().getVector(0).getObject(0)).isEqualTo(42L);
                        assertThat(stream.next()).isFalse();
                    }
                    var info = tasks.admittedTask(request);
                    tasks.awaitState(info, org.pangrova.worker.enums.TaskState.FINISHED);
                    tasks.manager().releaseTaskExecutor(info.taskId());
                    assertThat(info.executorHandle()).isZero();
                    assertThat(info.allocatedBufferMemory()).isZero();
                } finally {
                    server.stop();
                }
            }
        }
    }

    @Test
    void nativePlanFailureBeforeFlightRegistrationReturnsTheCauseAndDrains() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "http-output-setup-failure")) {
            var config = new WorkerHttpServer.WorkerHttpConfig(0, "127.0.0.1", runtime.resultPort())
                    .withBindHost("127.0.0.1");
            var server = new WorkerHttpServer(config, tasks.manager(), runtime.transports().exchangeTransport(),
                    null, null, runtime.resultTickets());
            server.start();
            try {
                var plan = io.substrait.proto.Plan.parseFrom(
                        ActualNativeTaskFixture.valuesTaskRequest(true).getSubstraitPlan()).toBuilder();
                plan.getVersionBuilder().setMajorNumber(99);
                var request = runtime.bindWorker(ActualNativeTaskFixture.planTaskRequest(true, plan.build().toByteArray()));
                var submit = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort() + "/v1/task"))
                        .timeout(Duration.ofSeconds(10)).header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(SubmitTaskRequest.from(request).encode())).build();
                var client = HttpClient.newHttpClient();
                var original = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                var replay = client.sendAsync(submit, HttpResponse.BodyHandlers.ofByteArray());
                var first = original.get(5, TimeUnit.SECONDS);
                var duplicate = replay.get(5, TimeUnit.SECONDS);
                assertThat(first.statusCode()).isEqualTo(500);
                assertThat(duplicate.statusCode()).isEqualTo(500);
                var ack = TaskSubmitAck.parseFrom(first.body());
                assertThat(ack.hasAdmitted()).isFalse();
                assertThat(ack.getRejected().getReason()).contains("Substrait spec major-version mismatch");
                assertThat(TaskSubmitAck.parseFrom(duplicate.body())).isEqualTo(ack);
                var info = tasks.admittedTask(request);
                tasks.awaitState(info, org.pangrova.worker.enums.TaskState.FAILED);
                assertThat(info.executorHandle()).isZero();
                assertThat(info.allocatedBufferMemory()).isZero();
                assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            } finally {
                server.stop();
            }
        }
    }

    private static TaskExecutor holdResultStart(ActualNativeTaskFixture runtime, CompletableFuture<Runnable> start) {
        return (TaskExecutor) Proxy.newProxyInstance(TaskExecutor.class.getClassLoader(), new Class<?>[]{TaskExecutor.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("startResultTask")) {
                        long handle = (long) args[0];
                        var onReady = (Runnable) args[1];
                        assertThat(start.complete(() -> runtime.executor().startResultTask(handle, onReady))).isTrue();
                        return null;
                    }
                    try {
                        return method.invoke(runtime.executor(), args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static java.util.concurrent.Semaphore pendingAcknowledgements(WorkerHttpServer server) throws Exception {
        var field = WorkerHttpServer.class.getDeclaredField("taskResource");
        field.setAccessible(true);
        var resource = field.get(server);
        var waiting = resource.getClass().getDeclaredField("pendingRetryAcks");
        waiting.setAccessible(true);
        return (java.util.concurrent.Semaphore) waiting.get(resource);
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
            if (!permit.await(25, TimeUnit.SECONDS)) {
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
