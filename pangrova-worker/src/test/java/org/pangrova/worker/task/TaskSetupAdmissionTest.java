package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.http.endpoint.TaskResource;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskSetupAdmissionTest {
    private enum Invalidation { EXACT_SEAL, ADVANCED_CONTROL_EPOCH }

    @ParameterizedTest
    @EnumSource(Invalidation.class)
    void queuedSetupRechecksAuthorityBeforeAcquiringNativeOwnership(Invalidation invalidation) throws Exception {
        var occupied = new CountDownLatch(64);
        var releaseSetups = new CountDownLatch(1);
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "queued-task-admission");
                var resource = new TaskResource(tasks.manager(), runtime.transports().exchangeTransport(),
                        new ExchangeSourceRegistry(), "127.0.0.1", runtime.resultPort(), null,
                        new TaskResource.ResultBinding("grpc", runtime.resultTickets()))) {
            var manager = tasks.manager();
            var request = runtime.valuesRequest(true);
            long epoch = request.getAttempt().getControlEpoch();
            manager.requireControlEpochHandshake();
            manager.observeControlEpoch(epoch);
            for (int slot = 0; slot < 64; slot++) {
                manager.scheduleTaskSetup(1, () -> holdSetupSlot(occupied, releaseSetups));
            }
            try {
                assertThat(occupied.await(8, TimeUnit.SECONDS)).isTrue();
                var submitted = resource.submitTask(new ByteArrayInputStream(SubmitTaskRequest.from(request).encode()), null)
                        .toCompletableFuture();
                assertThat(submitted).isNotDone();
                String taskId = request.getAttempt().getTaskId();
                byte[] seal = CanonicalTaskAdmissionSeals.create(request).toByteArray();
                switch (invalidation) {
                    case EXACT_SEAL -> {
                        try (var sealing = resource.cancelTask(taskId, seal)) {
                            assertThat(sealing.getStatus()).isEqualTo(202);
                        }
                    }
                    case ADVANCED_CONTROL_EPOCH -> manager.observeControlEpoch(epoch + 1);
                }
                releaseSetups.countDown();
                try (var rejected = submitted.get(10, TimeUnit.SECONDS)) {
                    assertThat(rejected.getStatus()).isEqualTo(409);
                    var ack = TaskSubmitAck.parseFrom((byte[]) rejected.getEntity());
                    assertThat(ack.hasRejected()).isTrue();
                }
                assertThat(manager.resolveWorkerTaskId(taskId)).isNull();
                assertThat(runtime.nodePool().getUsedBytes()).isZero();
                assertThat(runtime.arbitrator().getUsedMemory()).isZero();
                if (invalidation == Invalidation.EXACT_SEAL) {
                    try (var drained = resource.cancelTask(taskId, seal)) {
                        assertThat(drained.getStatus()).isEqualTo(200);
                        var receipt = TaskReport.parseFrom((byte[]) drained.getEntity());
                        assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
                        assertThat(receipt.getExecutionDrainState())
                                .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
                    }
                }
            } finally {
                releaseSetups.countDown();
            }
        }
    }

    private static void holdSetupSlot(CountDownLatch occupied, CountDownLatch releaseSetups) {
        occupied.countDown();
        try {
            if (!releaseSetups.await(20, TimeUnit.SECONDS)) {
                throw new AssertionError("Setup fixture did not release its threads");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Setup fixture was interrupted", failure);
        }
    }
}
