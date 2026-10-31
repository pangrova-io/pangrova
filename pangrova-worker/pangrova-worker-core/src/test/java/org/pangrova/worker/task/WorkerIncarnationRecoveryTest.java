package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerIncarnationRecoveryTest {
    @TempDir Path directory;

    @Test
    void retiredIncarnationClosesEvenNeverDeliveredExpiredOldEpochRequest() {
        TaskRequest old;
        try (var first = new TaskManager(ProcessIncarnationStore.open(directory))) {
            first.requireControlEpochHandshake();
            first.observeControlEpoch(1);
            old = request(first).toBuilder().setDeadlineEpochMs(1).build();
            assertThat(first.sealAdmission(CanonicalTaskAdmissionSeals.create(old))).isEmpty();
        }
        try (var restarted = new TaskManager(ProcessIncarnationStore.open(directory))) {
            restarted.requireControlEpochHandshake();
            restarted.observeControlEpoch(2);
            var receipt = restarted.sealAdmission(CanonicalTaskAdmissionSeals.create(old)).orElseThrow();
            assertThat(receipt.getAttempt()).isEqualTo(old.getAttempt());
            assertThat(receipt.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_INCARNATION_RETIRED);
            assertThatThrownBy(() -> restarted.verifyWorkerExecutionIdentity(old.getAttempt()))
                    .isInstanceOf(TaskAdmissionException.class);
            var forged = old.toBuilder().setAttempt(old.getAttempt().toBuilder().setWorkerExecutionIdentity(
                    old.getAttempt().getWorkerExecutionIdentity().toBuilder()
                            .setIncarnationId("00000000-0000-0000-0000-000000000099"))).build();
            assertThatThrownBy(() -> restarted.sealAdmission(CanonicalTaskAdmissionSeals.create(forged)))
                    .isInstanceOf(TaskAdmissionException.class);
            var unknownCurrent = request(restarted);
            assertThatThrownBy(() -> restarted.sealAdmission(CanonicalTaskAdmissionSeals.create(unknownCurrent)))
                    .isInstanceOf(ControlEpochFenceException.class);
        }
    }

    @Test
    void liveCurrentIncarnationStillRequiresExactAdmissionSealAndCannotBeRebuilt() {
        var store = ProcessIncarnationStore.open(directory);
        try (var manager = new TaskManager(store)) {
            assertThatThrownBy(() -> new TaskManager(store)).hasMessageContaining("already claimed");
            var request = request(manager);
            var exact = CanonicalTaskAdmissionSeals.create(request);
            assertThat(manager.sealAdmission(exact).orElseThrow().getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            assertThatThrownBy(() -> manager.taskAdmissions.reserve(request, "late"))
                    .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("sealed");
        }
    }

    private static TaskRequest request(TaskManager manager) {
        var builder = CanonicalTaskRequestTestFixture.builder("task", "execution", 0, 0,
                TaskKind.TASK_KIND_READ, new byte[] {1});
        return builder.setAttempt(builder.getAttempt().toBuilder()
                        .setWorkerExecutionIdentity(manager.workerExecutionIdentity().toProto()))
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId("grant")
                        .setCpuSlots(1).setNativeMemoryBytes(1024)).build();
    }
}
