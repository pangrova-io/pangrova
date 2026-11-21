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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerCdcTaskDispatcherTest {

    @Test
    void registersAndDispatchesTheExactCdcTask() {
        var tracker = new CoordinatorTaskTracker();
        var dispatched = new AtomicReference<SubmitTaskRequest>();
        TaskDispatcher transport = (request, worker) -> {
            dispatched.set(request);
            return new org.pangrova.execution.task.DispatchResult(
                    worker.host(), request.taskRequest().getAttempt().getTaskId(), null);
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
        var adapter = new WorkerCdcTaskDispatcher(transport, selector, tracker);
        var request = CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", 0, 0, TaskKind.TASK_KIND_CDC_INGEST,
                new byte[]{1}).build();

        adapter.dispatch(request);

        assertThat(dispatched.get().taskRequest().getAttempt().getTaskId()).isEqualTo("task-1");
        assertThat(tracker.getTaskRequest("task-1")).isNotNull();
        assertThat(tracker.getTaskState("query-1", "task-1").name()).isEqualTo("SCHEDULED");
    }

    private static WorkerInfo worker() {
        return new WorkerInfo("w-1", "localhost", 8080, 0, 0,
                org.pangrova.scheduler.api.enums.HealthState.ACTIVE,
                org.pangrova.common.enums.MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                java.time.Instant.now(), 0.9, null, 2, null, java.util.List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(),
                TestWorkerInfoBuilder.executionIdentity("w-1"));
    }
}
