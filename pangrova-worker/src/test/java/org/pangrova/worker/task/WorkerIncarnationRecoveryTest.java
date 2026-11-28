package org.pangrova.worker.task;

import org.pangrova.worker.exception.ControlEpochFenceException;
import org.pangrova.worker.exception.TaskAdmissionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;

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
            first.observeControlEpoch(2);
            var seal = CanonicalTaskAdmissionSeals.create(old);
            var expired = first.sealAdmission(seal).orElseThrow();
            assertThat(expired.getAttempt()).isEqualTo(old.getAttempt());
            assertThat(expired.getError().getCode()).isEqualTo("TASK_ADMISSION_EXPIRED_WITHOUT_LIVE_OWNER");
            assertThat(expired.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            assertThatThrownBy(() -> first.sealAdmission(CanonicalTaskAdmissionSeals.create(request(first))))
                    .isInstanceOf(ControlEpochFenceException.class);
            var future = old.toBuilder().setAttempt(old.getAttempt().toBuilder().setControlEpoch(3)).build();
            assertThatThrownBy(() -> first.sealAdmission(CanonicalTaskAdmissionSeals.create(future)))
                    .isInstanceOf(ControlEpochFenceException.class);
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

    @Test
    void httpSealStaysPendingUntilTheRegisteredOwnerActuallyExits() throws Exception {
        try (var worker = TaskManager.forHttpWiringOnly()) {
            worker.requireControlEpochHandshake();
            worker.observeControlEpoch(1);
            var task = request(worker);
            var admission = worker.taskAdmissions.reserve(task, "setup-in-flight");
            admission.publish(() -> {});
            var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0),
                    worker, TestExchangeTransport.INSTANCE);
            server.start();
            try {
                var seal = CanonicalTaskAdmissionSeals.create(task);
                var delete = HttpRequest.newBuilder(URI.create("http://localhost:" + server.getPort()
                                + "/v1/task/" + task.getAttempt().getTaskId()))
                        .timeout(Duration.ofSeconds(5)).header("Content-Type", SubmitTaskRequest.MEDIA_TYPE)
                        .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(seal.toByteArray())).build();
                var client = HttpClient.newHttpClient();
                var pending = client.send(delete, HttpResponse.BodyHandlers.ofByteArray());
                assertThat(pending.statusCode()).isEqualTo(202);
                assertThat(pending.body()).isEmpty();

                admission.rejectedBeforeOwnership(new IllegalStateException("Setup rolled back"));
                var released = client.send(delete, HttpResponse.BodyHandlers.ofByteArray());
                assertThat(released.statusCode()).isEqualTo(200);
                var receipt = TaskReport.parseFrom(released.body());
                assertThat(receipt.getAttempt()).isEqualTo(task.getAttempt());
                assertThat(receipt.getError().getCode()).isEqualTo("TASK_ADMISSION_CLOSED");
            } finally {
                if (!admission.completed()) {
                    admission.rejectedBeforeOwnership(new IllegalStateException("Test stopped before handoff"));
                }
                server.stop();
            }
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
