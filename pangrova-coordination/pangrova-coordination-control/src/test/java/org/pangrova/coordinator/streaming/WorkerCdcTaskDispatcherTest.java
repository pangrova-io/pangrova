package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.query.api.exception.JournalWriteException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerCdcTaskDispatcherTest {

    @Test
    void registersAndDispatchesTheExactCdcTask() {
        var tracker = new CoordinatorTaskTracker();
        var dispatched = new AtomicReference<SubmitTaskRequest>();
        var committed = new AtomicReference<List<TaskStateEvent>>();
        var journal = new InMemoryQueryStateJournal();
        journal.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(1, "issuer"),
                "query-1", null, null, "RUNNING"));
        TaskDispatcher transport = new TaskDispatcher() {
            @Override
            public SubmitTaskRequest bindControlIdentity(SubmitTaskRequest request) {
                return request.withBoundTask(request.taskRequest().toBuilder()
                        .setAttempt(request.taskRequest().getAttempt().toBuilder().setControlEpoch(19)).build());
            }

            @Override
            public org.pangrova.execution.task.DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
                throw new AssertionError("CDC must send the already-bound request");
            }

            @Override
            public org.pangrova.execution.task.DispatchResult dispatchBound(SubmitTaskRequest request, WorkerInfo worker) {
                assertThat(committed.get()).singleElement().satisfies(event -> {
                    assertThat(event.taskId()).isEqualTo(request.taskRequest().getAttempt().getTaskId());
                    assertThat(event.issuance().workerHost()).isEqualTo(worker.host());
                    assertThat(event.issuance().workerHttpPort()).isEqualTo(worker.httpPort());
                    assertThat(event.issuance().admissionSeal())
                            .isEqualTo(CanonicalTaskAdmissionSeals.create(request.taskRequest()).toByteArray());
                });
                dispatched.set(request);
                return new org.pangrova.execution.task.DispatchResult(
                        worker.host(), request.taskRequest().getAttempt().getTaskId(), null);
            }
        };
        WorkerSelector selector = new WorkerSelector() {
            @Override
            public WorkerInfo select(String tenantId) {
                return worker();
            }

            @Override
            public org.pangrova.protocol.v2.StageResourceGrant computeResourceLimit(
                    int parallelism, int concurrentStages, WorkerInfo selectedWorker) {
                return org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("grant").setCpuSlots(1)
                        .setNativeMemoryBytes(1_048_576).build();
            }
        };
        var adapter = new WorkerCdcTaskDispatcher(transport, selector, tracker, facts -> {
            journal.appendTaskIssuancesSync(facts);
            committed.set(facts);
        });
        var builder = CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", 0, 0, TaskKind.TASK_KIND_CDC_INGEST,
                new byte[0])
                .clearSubstraitPlanChecksum()
                .setSecurity(org.pangrova.protocol.v2.SecurityContext.newBuilder()
                        .setTenantId("tenant-a").setPrincipal("test-user"))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("task-1:grant").setCpuSlots(1).setNativeMemoryBytes(1_048_576))
                .setCdcIngest(CdcTaskRequestsTest.binding());
        builder.setAttempt(builder.getAttempt().toBuilder().clearWorkerExecutionIdentity());

        adapter.dispatch(builder.build());

        assertThat(dispatched.get().taskRequest().getAttempt().getTaskId()).isEqualTo("task-1");
        assertThat(dispatched.get().taskRequest().getAttempt().getControlEpoch()).isEqualTo(19);
        assertThat(dispatched.get().taskRequest().getAttempt().getWorkerExecutionIdentity())
                .isEqualTo(worker().workerExecutionIdentity().toProto());
        assertThat(tracker.getTaskRequest("task-1")).isSameAs(dispatched.get());
        assertThat(tracker.getTaskState("query-1", "task-1").name()).isEqualTo("SCHEDULED");

        journal.close();
        builder.setAttempt(builder.getAttempt().toBuilder().setTaskId("rejected-task"));
        assertThatThrownBy(() -> adapter.dispatch(builder.build())).isInstanceOf(JournalWriteException.class)
                .hasMessageContaining("journal is closed");
        assertThat(tracker.getTaskRequest("rejected-task")).isNull();
        assertThat(dispatched.get().taskRequest().getAttempt().getTaskId()).isEqualTo("task-1");
    }

    private static WorkerInfo worker() {
        return new WorkerInfo("w-1", "localhost", 8080, 0, 0,
                org.pangrova.scheduler.api.enums.HealthState.ACTIVE,
                org.pangrova.common.enums.MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                java.time.Instant.now(), 0.9, null, 2, null, java.util.List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(), 0,
                TestWorkerInfoBuilder.executionIdentity("w-1"), null, true);
    }
}
