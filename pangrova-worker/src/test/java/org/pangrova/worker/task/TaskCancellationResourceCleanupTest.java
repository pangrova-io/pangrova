package org.pangrova.worker.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;
import org.pangrova.testing.nativefixture.ActualPausedNativeResultReader;
import org.pangrova.worker.execution.ActualNativeHandleLease;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual JNI, Flight owners and production FSM/release controllers; not a distributed SQL E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskCancellationResourceCleanupTest {
    @Test
    void cancelledTaskReleasesExecutorHandle() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel")) {
            var info = tasks.submit(largeRequest());
            long handle = info.executorHandle();
            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
            tasks.awaitState(info, TaskManager.TaskState.CANCELLED);
            assertRetired(runtime, info, handle);
            var report = TaskReport.parseFrom(tasks.terminalReport(info).canonicalTaskReport());
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
            assertThat(report.getAttempt()).isEqualTo(info.request().taskRequest().getAttempt());
        }
    }

    @Test
    void completionMonitorReleasesResourcesForCancelledTask() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancelled-callback")) {
            var info = tasks.preparePending(runtime.valuesRequest(false));
            long handle = info.executorHandle();
            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
            var monitor = tasks.completionMonitor();
            Runnable release = () -> tasks.manager().releaseTaskExecutor(info.taskId());
            monitor.onTaskCompleted(info.taskId(), info, tasks.manager().lifecycleCallback, release);
            tasks.awaitState(info, TaskManager.TaskState.CANCELLED);
            byte[] receipt = info.executorReleaseReport();
            // A duplicate actual callback after CANCELLED still joins the same
            // real release owner rather than returning before resource cleanup.
            monitor.onTaskCompleted(info.taskId(), info, tasks.manager().lifecycleCallback, release);
            assertThat(info.executorReleaseReport()).isEqualTo(receipt);
            assertRetired(runtime, info, handle);
            assertThat(tasks.reports().reports().stream().filter(report -> report.state().equals("CANCELLED")))
                    .hasSize(1);
        }
    }

    @Test
    void fullLifecycleCancelReleasesAllResources() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-full")) {
            var info = tasks.submit(largeRequest());
            long handle = info.executorHandle();
            assertThat(info.allocatedBufferMemory()).isPositive();
            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
            tasks.awaitState(info, TaskManager.TaskState.CANCELLED);
            assertRetired(runtime, info, handle);
            assertThat(runtime.executor().releaseTask(handle)).isEmpty();
            assertThat(TaskReport.parseFrom(info.executorReleaseReport()).getState())
                    .isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isFalse();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
        }
    }

    @Test
    void cancellationWaitsForNativeJoinBeforeReportAndBudgetRelease() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-lease")) {
            var info = tasks.preparePending(runtime.valuesRequest(false));
            long handle = info.executorHandle();
            try (var lease = new ActualNativeHandleLease(runtime.executor(), handle)) {
                assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
                var failure = new AtomicReference<Throwable>();
                Thread completion = completionThread(tasks, info, failure);
                completion.start();
                await().atMost(Duration.ofSeconds(10)).until(() -> lease.blocks(completion));
                assertThat(info.getState()).isEqualTo(TaskManager.TaskState.CANCELLING);
                assertThat(info.executorHandle()).isEqualTo(handle);
                assertThat(info.allocatedBufferMemory()).isPositive();
                assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("CANCELLED"));
                lease.close();
                completion.join(10_000);
                assertThat(completion.isAlive()).isFalse();
                assertThat(failure.get()).isNull();
            }
            tasks.awaitState(info, TaskManager.TaskState.CANCELLED);
            assertRetired(runtime, info, handle);
        }
    }

    @Test
    void failedNativeJoinKeepsBudgetAndRetriesAfterActualRpcDrains() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-rpc")) {
            var request = largeRequest();
            var info = tasks.submit(request);
            long handle = info.executorHandle();
            try (var reader = new ActualPausedNativeResultReader(runtime, request)) {
                assertThat(reader.firstBatchRows()).isPositive();
                await().atMost(Duration.ofSeconds(20))
                        .until(() -> runtime.drainEvidence(handle, request).getActiveReaders() > 0);
                assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
                await().atMost(Duration.ofSeconds(20))
                        .until(() -> runtime.drainEvidence(handle, request).getProducerStopped());
                assertThatThrownBy(() -> tasks.manager().releaseTaskExecutor(info.taskId()))
                        .isInstanceOf(RuntimeException.class).hasMessageContaining("Result RPC");
                assertThat(info.executorHandle()).isEqualTo(handle);
                assertThat(info.allocatedBufferMemory()).isPositive();
                assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("CANCELLED"));
            }
            // The one production cleanup sweep retries the actual failed
            // controller continuation. There is no fake 'next release succeeds'.
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                tasks.manager().cleanupTerminalTasks(Duration.ofDays(1));
                assertThat(info.getState()).isEqualTo(TaskManager.TaskState.CANCELLED);
            });
            assertRetired(runtime, info, handle);
        }
    }

    @Test
    void cancellationDuringNativeCreationNeverStartsOrReportsBeforeRelease() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-create-cancel")) {
            // Exercise the actual created-but-not-started admission boundary.
            // The canonical Native receipt proves no producer Task was started.
            var info = tasks.preparePending(runtime.valuesRequest(false));
            long handle = info.executorHandle();
            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
            assertThat(info.allocatedBufferMemory()).isPositive();
            tasks.manager().lifecycleManager.executeTask(info.taskId(), info, tasks.manager().lifecycleCallback);
            tasks.awaitState(info, TaskManager.TaskState.CANCELLED);
            assertRetired(runtime, info, handle);
            var receipt = TaskReport.parseFrom(info.executorReleaseReport());
            assertThat(receipt.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(receipt.hasNativeTaskStats()).isFalse();
            assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("RUNNING"));
        }
    }

    @Test
    void cancellationRacingCompletedCallbackCannotPublishFinished() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-wins")) {
            var info = tasks.preparePending(runtime.valuesRequest(false));
            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
            assertThat(tasks.manager().lifecycleCallback.transition(info.taskId(), TaskManager.TaskState.FINISHING))
                    .isFalse();
            tasks.completionMonitor().onTaskCompleted(info.taskId(), info, tasks.manager().lifecycleCallback,
                    () -> tasks.manager().releaseTaskExecutor(info.taskId()));
            tasks.awaitState(info, TaskManager.TaskState.CANCELLED);
            assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("FINISHED"));
            assertThat(info.executorHandle()).isZero();
        }
    }

    @Test
    void completionWinsBeforeCancellationAtFinishingCommitPoint() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-finish-wins")) {
            var info = tasks.preparePending(runtime.valuesRequest(false));
            long handle = info.executorHandle();
            runtime.executor().startTask(handle);
            runtime.awaitFinished(handle);
            assertThat(tasks.manager().transition(info.taskId(), TaskManager.TaskState.RUNNING)).isTrue();
            info.setTerminalTaskReport(runtime.executor().getTaskStatus(handle));
            info.setTerminalTaskStats(runtime.executor().getTaskStats(handle));
            var failure = new AtomicReference<Throwable>();
            Thread completion = completionThread(tasks, info, failure);
            try (var lease = new ActualNativeHandleLease(runtime.executor(), handle)) {
                completion.start();
                await().atMost(Duration.ofSeconds(10)).until(() -> lease.blocks(completion));
                assertThat(info.getState()).isEqualTo(TaskManager.TaskState.FINISHING);
                assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isFalse();
                assertThat(info.allocatedBufferMemory()).isPositive();
            }
            completion.join(10_000);
            assertThat(completion.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertRetired(runtime, info, handle);
            assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("CANCELLED"));
        }
    }

    private static org.pangrova.protocol.v2.TaskRequest largeRequest() {
        return ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.largeCross(4096));
    }

    private static Thread completionThread(ActualTaskManagerFixture tasks, TaskInfo info,
                                            AtomicReference<Throwable> failure) {
        return new Thread(() -> {
            try {
                tasks.completionMonitor().onTaskCompleted(info.taskId(), info, tasks.manager().lifecycleCallback,
                        () -> tasks.manager().releaseTaskExecutor(info.taskId()));
            } catch (Throwable observed) { failure.set(observed); }
        }, "actual-terminal-release");
    }

    private static void assertRetired(ActualNativeTaskFixture runtime, TaskInfo info, long oldHandle) {
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(info.executorReleaseReport()).isNotEmpty();
        assertThatThrownBy(() -> runtime.executor().getTaskStatus(oldHandle)).isInstanceOf(IllegalStateException.class);
    }
}
