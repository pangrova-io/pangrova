package org.pangrova.worker.task;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.task.memory.MemoryRefs;

import java.time.Duration;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual JNI task manager assembly with production resource and transport owners. */
public final class ActualTaskManagerFixture implements AutoCloseable {
    private final ActualNativeTaskFixture runtime;
    private final TaskManager manager;
    private final RecordingTaskStatusReporter reports;
    private final TaskManagerContext context;

    public ActualTaskManagerFixture(ActualNativeTaskFixture runtime, String workerId) {
        this(runtime, workerId, new RecordingTaskStatusReporter(), new MemoryRefs(
                new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager()),
                runtime.nodePool(), null));
    }

    /** A distinct real admission budget is permitted for pre-registration rejection tests. */
    public ActualTaskManagerFixture(ActualNativeTaskFixture runtime, String workerId,
                                    RecordingTaskStatusReporter reports, MemoryRefs memory) {
        this.runtime = runtime;
        this.reports = reports;
        var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "actual-jni-task-monitor"));
        context = new TaskManagerContext(reports, runtime.executor(), memory, runtime.transports(), monitor);
        manager = new TaskManager(context, workerId, 0.0);
    }

    public TaskManager manager() { return manager; }
    public RecordingTaskStatusReporter reports() { return reports; }

    /** Real admission and executor ownership, with completion deliberately driven by its actual controller test. */
    TaskInfo preparePending(TaskRequest request) {
        var submitted = SubmitTaskRequest.from(request);
        var descriptor = TaskRequestDtoConverter.toTaskDescriptor(submitted);
        var info = new TaskInfo("pending-" + request.getAttempt().getTaskId(),
                request.getAttempt().getQueryOrJobId(), submitted, descriptor, java.time.Instant.now());
        manager.lifecycleManager.reserveAdmissionResources(info);
        manager.tasks.put(info.taskId(), info);
        manager.transition(info.taskId(), TaskManager.TaskState.INITIALIZING);
        info.setExecutorHandle(runtime.createPending(request));
        return info;
    }

    TaskCompletionMonitor completionMonitor() { return new TaskCompletionMonitor(context, manager.statusCollector); }

    public TaskInfo submit(TaskRequest request) {
        var submitted = SubmitTaskRequest.from(request);
        String id = manager.submitTask(TaskRequestDtoConverter.toTaskDescriptor(submitted), submitted);
        return manager.tasks.get(id);
    }

    public void awaitState(TaskInfo info, TaskManager.TaskState expected) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(info.getState()).isEqualTo(expected);
            assertThat(reports.reports()).anyMatch(report -> report.taskId()
                    .equals(info.request().taskRequest().getAttempt().getTaskId())
                    && report.state().equals(expected.name()));
        });
    }

    public TaskStatusReport terminalReport(TaskInfo info) {
        return reports.reports().stream().filter(report -> report.taskId()
                .equals(info.request().taskRequest().getAttempt().getTaskId())
                && (report.state().equals("FINISHED") || report.state().equals("FAILED")
                    || report.state().equals("CANCELLED"))).findFirst().orElseThrow();
    }

    public ActualNativeResultReader.Received consume(TaskInfo info) throws Exception {
        var received = ActualNativeResultReader.read(runtime, info.request().taskRequest());
        long handle = info.executorHandle();
        await().atMost(Duration.ofSeconds(20))
                .until(() -> runtime.drainEvidence(handle, info.request().taskRequest()).getDrained());
        manager.releaseTaskExecutor(info.taskId());
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        return received;
    }

    @Override
    public void close() {
        manager.close();
    }
}
