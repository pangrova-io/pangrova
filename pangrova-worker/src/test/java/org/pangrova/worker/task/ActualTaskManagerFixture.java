package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
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
        this(runtime, workerId, reports, memory, runtime.transports());
    }

    ActualTaskManagerFixture(ActualNativeTaskFixture runtime, String workerId, RecordingTaskStatusReporter reports,
            MemoryRefs memory, TransportRefs transports) {
        this(runtime, workerId, reports, memory, transports, runtime.executor());
    }

    ActualTaskManagerFixture(ActualNativeTaskFixture runtime, String workerId, RecordingTaskStatusReporter reports,
            MemoryRefs memory, TransportRefs transports, org.pangrova.worker.execution.TaskExecutor executor) {
        this.runtime = runtime;
        this.reports = reports;
        var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "actual-jni-task-monitor"));
        context = new TaskManagerContext(reports, executor, memory, transports, monitor);
        manager = new TaskManager(context, workerId, 0.0, runtime.admissionIncarnation());
    }

    public TaskManager manager() { return manager; }
    public RecordingTaskStatusReporter reports() { return reports; }

    /** Real admission and executor ownership, with completion deliberately driven by its actual controller test. */
    public TaskInfo preparePending(TaskRequest request) {
        var submitted = SubmitTaskRequest.from(request);
        var info = new TaskInfo("pending-" + request.getAttempt().getTaskId(),
                submitted, java.time.Instant.now());
        // Same canonical reservation as production submission, while retaining the real not-started boundary.
        manager.verifyControlEpochForAdmission(request.getAttempt().getControlEpoch());
        manager.verifyExecutionOwnerForAdmission(submitted);
        var reservation = manager.taskAdmissions.reserve(request, info.taskId());
        assertThat(reservation.first()).isTrue();
        manager.lifecycleManager.reserveAdmissionResources(info);
        reservation.publish(() -> manager.tasks.register(info));
        assertThat(manager.transition(info.taskId(), TaskState.INITIALIZING)).isTrue();
        reservation.admitted();
        assertThat(manager.taskAdmissions.knownIdentity(
                org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(request))).isTrue();
        info.beginNativeCreation();
        try {
            info.setExecutorHandle(runtime.createPending(request));
        } catch (RuntimeException | Error failure) {
            info.failNativeCreation(failure);
            throw failure;
        }
        return info;
    }

    TaskCompletionMonitor completionMonitor() { return new TaskCompletionMonitor(context, manager.statusCollector); }

    /** Reads the real HTTP admission's exact task owner without resubmitting or synthesizing state. */
    public TaskInfo admittedTask(TaskRequest request) {
        var owner = java.util.Objects.requireNonNull(manager.tasks.get(
                manager.resolveWorkerTaskId(request.getAttempt().getTaskId())), "actual admitted task");
        if (!owner.request().taskRequest().equals(request)) {
            throw new IllegalArgumentException("Admitted task differs from the exact requested owner");
        }
        return owner;
    }

    public TaskInfo submit(TaskRequest request) {
        var submitted = SubmitTaskRequest.from(request);
        String id = manager.submitTask(submitted);
        return manager.tasks.get(id);
    }

    public void awaitState(TaskInfo info, TaskState expected) {
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
