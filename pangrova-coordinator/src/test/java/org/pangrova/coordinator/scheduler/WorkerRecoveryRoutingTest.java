package org.pangrova.coordinator.scheduler;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.worker.WorkerExecutionIdentity;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.WorkerIncarnationControlFixture;

import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP admission-ledger recovery; Native process death has a separate independent-process fixture. */
class WorkerRecoveryRoutingTest {
    @TempDir Path directory;

    @Test
    void followsSameAdmissionDomainToANewPortWithoutRebindingTheOriginalAttempt() throws Exception {
        WorkerExecutionIdentity original;
        int oldPort;
        try (var first = new WorkerIncarnationControlFixture(directory)) {
            original = first.manager().workerExecutionIdentity();
            var server = server(first);
            try {
                oldPort = server.getPort();
            } finally {
                server.stop();
            }
        }
        AtomicInteger oldRequests = new AtomicInteger();
        HttpServer oldAddress = HttpServer.create(new InetSocketAddress("localhost", oldPort), 0);
        oldAddress.createContext("/", exchange -> {
            oldRequests.incrementAndGet();
            exchange.sendResponseHeaders(410, -1);
            exchange.close();
        });
        oldAddress.start();
        try (var replacement = new WorkerIncarnationControlFixture(directory);
                var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider())) {
            replacement.manager().requireControlEpochHandshake();
            replacement.manager().observeControlEpoch(2);
            var server = server(replacement);
            try {
                var identity = replacement.manager().workerExecutionIdentity();
                assertThat(identity.admissionDomainId()).isEqualTo(original.admissionDomainId());
                assertThat(identity.incarnationId()).isNotEqualTo(original.incarnationId());
                assertThat(server.getPort()).isNotEqualTo(oldPort);
                dispatcher.setRecoveryWorkers(() -> List.of(worker(server.getPort(), identity)));
                var issued = request(original);

                var result = dispatcher.cancelTask(issued, "localhost", oldPort);

                assertThat(result.isSuccess()).isTrue();
                assertThat(result.executionDrainReceipt().getAttempt()).isEqualTo(issued.taskRequest().getAttempt());
                assertThat(result.executionDrainReceipt().getExecutionDrainState())
                        .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_INCARNATION_RETIRED);
                assertThat(oldRequests).hasValue(0);
            } finally {
                server.stop();
            }
        } finally {
            oldAddress.stop(0);
        }
    }

    @Test
    void ambiguousSuccessorsCannotProduceARecoveryReceipt() {
        var identity = TestWorkerInfoBuilder.executionIdentity("physical-worker");
        try (var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider())) {
            dispatcher.setRecoveryWorkers(() -> List.of(worker(10101, identity), worker(10102, identity)));

            var result = dispatcher.cancelTask(request(identity), "localhost", 1);

            assertThat(result.isSuccess()).isFalse();
            assertThat(result.executionDrainReceipt()).isNull();
        }
    }

    private static WorkerHttpServer server(WorkerIncarnationControlFixture fixture) throws Exception {
        var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0),
                fixture.manager(), TestExchangeTransport.INSTANCE);
        server.start();
        return server;
    }

    private static WorkerInfo worker(int port, WorkerExecutionIdentity identity) {
        return new WorkerInfo("replacement-worker", "localhost", port, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0, 1024L,
                Instant.now(), 0.0, null, 2, null, List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(), identity);
    }

    private static SubmitTaskRequest request(WorkerExecutionIdentity identity) {
        var request = CanonicalTaskRequestTestFixture.builder(
                "issued-task", "issued-query", 0, 0, TaskKind.TASK_KIND_READ, new byte[] {1});
        request.getAttemptBuilder().setWorkerExecutionIdentity(identity.toProto());
        request.setResourceGrant(StageResourceGrant.newBuilder().setGrantId("grant")
                .setCpuSlots(1).setNativeMemoryBytes(1024));
        return SubmitTaskRequest.from(request.build());
    }
}
