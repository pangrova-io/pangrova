package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.coordinator.query.task.report.CanonicalTaskReportValidator;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskKind;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Observes one real query and delays actual terminal reports without manufacturing execution state. */
final class SpeculativeProcessScenario implements AutoCloseable {
    enum Fault { NONE, COPY_READ, ORIGINAL_READ, BOTH_READ, COPY_DISPATCH }
    private final IndependentSpeculationProcesses processes;
    private final AtomicReference<IndependentSpeculationProcesses.IssuedTask> original = new AtomicReference<>();
    private final AtomicBoolean observedActiveReservation = new AtomicBoolean();
    private final ProcessFileFault files;
    private final ProcessStatusReportRelay.ReportHold originalReports;
    private final ProcessStatusReportRelay.ReportHold copyReports;
    private final CompletableFuture<IndependentSpeculationProcesses.SqlOutput> output;
    private final CopyOnWriteArrayList<Throwable> faultFailures = new CopyOnWriteArrayList<>();

    SpeculativeProcessScenario(IndependentSpeculationProcesses processes, Fault fault, boolean holdCopy, String sql) {
        this.processes = processes;
        files = new ProcessFileFault(processes.input().dataFiles());
        originalReports = processes.reports().holdReports(report -> matchesOriginal(report) && terminal(report));
        copyReports = processes.reports().holdReports(report -> holdCopy && matchesCopy(report) && terminal(report));
        for (var worker : processes.workers()) {
            worker.http().beforeDelivery(transfer -> beforeDelivery(worker.workerId(), transfer, fault));
            worker.http().disconnectBeforeDelivery(transfer -> fault == Fault.COPY_DISPATCH && copySubmission(transfer));
        }
        output = processes.query(sql);
    }

    IndependentSpeculationProcesses.IssuedTask original() {
        await("real source task issued").atMost(Duration.ofSeconds(60)).until(() -> original.get() != null);
        return original.get();
    }

    IndependentSpeculationProcesses.IssuedTask copy() throws IOException {
        var selected = original();
        await("production speculative clone issued").atMost(Duration.ofSeconds(90)).until(() -> copies().size() == 1);
        var copy = copies().get(0);
        var from = selected.request().taskRequest();
        var actual = copy.request().taskRequest();
        assertThat(copy.workerId()).isNotEqualTo(selected.workerId());
        assertThat(actual.getAttempt().getQueryOrJobId()).isEqualTo(from.getAttempt().getQueryOrJobId());
        assertThat(actual.getAttempt().getStageId()).isEqualTo(from.getAttempt().getStageId());
        assertThat(actual.getAttempt().getPartitionId()).isEqualTo(from.getAttempt().getPartitionId());
        assertThat(actual.getSubstraitPlan()).isEqualTo(from.getSubstraitPlan());
        assertThat(actual.getSplitsList()).isEqualTo(from.getSplitsList());
        assertThat(actual.getSecurity()).isEqualTo(from.getSecurity());
        assertThat(copy.transfer().receivedNanos() - selected.transfer().receivedNanos())
                .as("observed wall time, not an invented duration snapshot").isGreaterThan(TimeUnit.SECONDS.toNanos(10));
        assertSourcePartitions();
        return copy;
    }

    ProcessStatusReportRelay.Delivery report(String taskId, String state) {
        await("actual Native terminal report").atMost(Duration.ofSeconds(60))
                .until(() -> reports(taskId, state).stream().anyMatch(delivery -> delivery.report().canonicalTaskReport() != null));
        return reports(taskId, state).stream().filter(delivery -> delivery.report().canonicalTaskReport() != null)
                .findFirst().orElseThrow();
    }

    void releaseOriginal() { originalReports.close(); }
    void releaseCopy() { copyReports.close(); }
    void restoreFiles() throws IOException { files.close(); }

    IndependentSpeculationProcesses.SqlOutput complete(boolean succeeded) throws Exception {
        assertThat(observedActiveReservation).as("actual persisted active query owner was observed before dispatch").isTrue();
        originalReports.close();
        copyReports.close();
        var result = output.get(5, TimeUnit.MINUTES);
        assertThat(result.succeeded()).as(result.output() + result.errors()).isEqualTo(succeeded);
        String queryId = original().request().taskRequest().getAttempt().getQueryOrJobId();
        assertThat(processes.queryStatus(queryId).state()).isEqualTo(succeeded ? "FINISHED" : "FAILED");
        await("real admission owner release").atMost(Duration.ofSeconds(60)).until(() -> processes.activeTenantReservations() == 0);
        assertThat(faultFailures).isEmpty();
        return result;
    }

    void awaitExecutionDrain(String taskId) {
        await("real exact attempt drain").atMost(Duration.ofSeconds(60)).until(() -> isReleased(taskId));
    }

    long successfulCancelCalls(String taskId) {
        return processes.workers().stream().flatMap(worker -> worker.http().transfers().stream())
                .filter(transfer -> transfer.method().equals("DELETE") && transfer.path().getPath().equals("/v1/task/" + taskId))
                .filter(transfer -> transfer.response().get() != null && transfer.response().get().statusCode() == 200).count();
    }

    private boolean isReleased(String taskId) throws IOException {
        var issued = tasks().stream().filter(task -> task.request().taskRequest().getAttempt().getTaskId().equals(taskId))
                .findFirst().orElseThrow();
        for (var delivery : processes.reports().deliveries()) {
            if (!delivery.report().taskId().equals(taskId) || delivery.report().canonicalTaskReport() == null) continue;
            var status = new CanonicalTaskReportValidator().validate(delivery.report(), issued.request()).orElseThrow();
            if (status.getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) return true;
        }
        for (var worker : processes.workers()) {
            for (var transfer : worker.http().transfers()) {
                if (!transfer.method().equals("DELETE") || !transfer.path().getPath().equals("/v1/task/" + taskId)) continue;
                var response = transfer.response().get();
                if (response != null && response.statusCode() == 200) {
                    new CanonicalTaskReportValidator().validateExecutionDrainReceipt(
                            response.body(), issued.request().taskRequest().getAttempt());
                    return true;
                }
            }
        }
        return false;
    }

    private void beforeDelivery(String workerId, ProcessWorkerHttpRelay.Transfer transfer, Fault fault) {
        if (!transfer.isTaskSubmission()) return;
        try {
            var request = transfer.submittedTask();
            if (request.taskRequest().getSplitsCount() == 0) return;
            if (!request.taskRequest().getAttempt().getTaskId().startsWith("spec-")) {
                var issued = new IndependentSpeculationProcesses.IssuedTask(workerId, request, transfer);
                if (original.compareAndSet(null, issued)) {
                    observedActiveReservation.set(processes.activeTenantReservations() > 0);
                    if (fault == Fault.ORIGINAL_READ || fault == Fault.BOTH_READ) files.hide(request);
                }
            } else if (matchesCopy(request.taskRequest().getAttempt().getTaskId(), request.taskRequest().getAttempt().getQueryOrJobId())) {
                if (fault == Fault.COPY_READ) files.hide(request);
                if (fault == Fault.ORIGINAL_READ) files.close();
            }
        } catch (IOException failure) {
            faultFailures.add(failure);
            throw new UncheckedIOException(failure);
        }
    }

    private boolean copySubmission(ProcessWorkerHttpRelay.Transfer transfer) {
        if (!transfer.isTaskSubmission()) return false;
        try {
            var attempt = transfer.submittedTask().taskRequest().getAttempt();
            return matchesCopy(attempt.getTaskId(), attempt.getQueryOrJobId());
        } catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private boolean matchesOriginal(TaskStatusReport report) {
        var issued = original.get();
        return issued != null && issued.request().taskRequest().getAttempt().getTaskId().equals(report.taskId());
    }
    private boolean matchesCopy(TaskStatusReport report) { return matchesCopy(report.taskId(), report.queryId()); }
    private boolean matchesCopy(String taskId, String queryId) {
        var issued = original.get();
        return issued != null && taskId.startsWith("spec-") && issued.request().taskRequest().getAttempt().getQueryOrJobId().equals(queryId);
    }
    private static boolean terminal(TaskStatusReport report) { return List.of("FINISHED", "FAILED", "CANCELLED").contains(report.state()); }

    private List<ProcessStatusReportRelay.Delivery> reports(String taskId, String state) {
        return processes.reports().deliveries().stream().filter(delivery -> delivery.report().taskId().equals(taskId)
                && delivery.report().state().equals(state)).toList();
    }
    private List<IndependentSpeculationProcesses.IssuedTask> tasks() throws IOException {
        String queryId = original().request().taskRequest().getAttempt().getQueryOrJobId();
        return processes.issuedTasks().stream().filter(task -> task.request().taskRequest().getAttempt().getQueryOrJobId().equals(queryId)).toList();
    }
    private List<IndependentSpeculationProcesses.IssuedTask> copies() throws IOException {
        return tasks().stream().filter(task -> task.request().taskRequest().getAttempt().getTaskId().startsWith("spec-"))
                .collect(java.util.stream.Collectors.toMap(task -> task.request().taskRequest().getAttempt().getTaskId(),
                        task -> task, (first, duplicate) -> first)).values().stream().toList();
    }
    private void assertSourcePartitions() throws IOException {
        var request = original().request().taskRequest();
        var identity = request.getAttempt();
        int expectedPartitions = request.getTaskKind() == TaskKind.TASK_KIND_WRITE ? 1 : 4;
        if (expectedPartitions == 1) {
            assertThat(request.getSplitsCount()).as("one fenced writer covers all four source splits").isEqualTo(4);
        }
        assertThat(tasks().stream().map(task -> task.request().taskRequest())
                .filter(source -> !source.getAttempt().getTaskId().startsWith("spec-")
                        && source.getAttempt().getStageId() == identity.getStageId())
                .map(source -> source.getAttempt().getPartitionId()).distinct().toList()).hasSize(expectedPartitions);
    }

    @Override public void close() throws Exception {
        originalReports.close();
        copyReports.close();
        Exception failure = null;
        try { files.close(); } catch (IOException restorationFailure) { failure = restorationFailure; }
        processes.workers().forEach(worker -> {
            worker.http().beforeDelivery(ignored -> { });
            worker.http().disconnectBeforeDelivery(ignored -> false);
        });
        processes.reports().releaseReports();
        try { output.get(5, TimeUnit.MINUTES); }
        catch (Exception queryFailure) {
            if (queryFailure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (failure == null) failure = queryFailure;
            else failure.addSuppressed(queryFailure);
        }
        if (failure != null) throw failure;
    }
}
