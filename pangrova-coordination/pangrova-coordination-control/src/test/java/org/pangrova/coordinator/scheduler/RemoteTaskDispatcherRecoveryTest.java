package org.pangrova.coordinator.scheduler;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.enums.SchedulerCancellationStatus;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.TaskIssuance;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteTaskDispatcherRecoveryTest {
    @Test
    void durableSealGetsExactClosureReceiptButPendingAndMismatchedIssuanceProveNothing() throws Exception {
        try (var worker = TaskManager.forHttpWiringOnly();
                var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider())) {
            worker.requireControlEpochHandshake();
            worker.observeControlEpoch(1);
            var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0);
            var server = new WorkerHttpServer(config, worker, TestExchangeTransport.INSTANCE);
            server.start();
            try {
                var request = request(worker, "issued");
                var original = issuance(request, server.getPort(), "execution");
                var drained = dispatcher.recoverTaskDrain(original).get(5, TimeUnit.SECONDS);
                assertThat(drained.status()).isEqualTo(SchedulerCancellationStatus.DRAINED);
                assertThat(drained.executionDrainReceipt().getAttempt()).isEqualTo(request.getAttempt());
                assertThat(drained.executionDrainReceipt().getExecutionDrainState())
                        .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);

                var expired = request(worker, "expired").toBuilder().setDeadlineEpochMs(1).build();
                var pending = dispatcher.recoverTaskDrain(issuance(expired,
                        server.getPort(), "execution")).get(5, TimeUnit.SECONDS);
                assertThat(pending.status()).isEqualTo(SchedulerCancellationStatus.PENDING);
                assertThat(pending.executionDrainReceipt()).isNull();

                var mismatched = dispatcher.recoverTaskDrain(issuance(request, server.getPort(), "other"))
                        .get(5, TimeUnit.SECONDS);
                assertThat(mismatched.status()).isEqualTo(SchedulerCancellationStatus.FAILED);
                assertThat(mismatched.errorMessage()).contains("changed its execution identity");
                assertThat(mismatched.executionDrainReceipt()).isNull();
            } finally {
                server.stop();
            }
        }
    }

    private static TaskStateEvent issuance(TaskRequest request, int port, String queryId) {
        var seal = CanonicalTaskAdmissionSeals.create(request);
        var owner = seal.getAttempt().getExecutionOwner();
        var issuance = new TaskIssuance("localhost", port, owner.getOwnerIncarnation(),
                owner.getFencingToken(), seal.toByteArray());
        return new TaskStateEvent(JournalEventContext.create(1, owner.getOwnerId()),
                queryId, seal.getAttempt().getTaskId(), "worker", "SCHEDULED", 0, 0, issuance);
    }

    private static TaskRequest request(TaskManager worker, String taskId) {
        var request = CanonicalTaskRequestTestFixture.builder(taskId, "execution", 0, 0,
                TaskKind.TASK_KIND_READ, new byte[] {1});
        return request.setAttempt(request.getAttempt().toBuilder()
                .setWorkerExecutionIdentity(worker.workerExecutionIdentity().toProto())).build();
    }
}
