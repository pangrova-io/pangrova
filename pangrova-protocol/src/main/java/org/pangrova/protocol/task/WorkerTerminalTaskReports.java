package org.pangrova.protocol.task;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.v2.ArtifactFreshnessReport;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.SourceSnapshotReport;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;

/** Fenced failure evidence produced by a Worker when Native cannot do so. */
public final class WorkerTerminalTaskReports {

    private static final int MAX_ERROR_MESSAGE_LENGTH = 4096;
    private static final int MAX_EXPLICIT_FAILED_WORK_UNITS =
            CanonicalTaskRequests.MAX_COVERAGE_IDENTITIES;

    private WorkerTerminalTaskReports() {}

    /**
     * Builds the sole successful JVM completion report accepted for a bounded Worker task.
     * It carries no Native statistics, no error, and the exact Worker-task drain state.
     */
    public static byte[] workerExecuted(TaskRequest request) {
        if (request == null || !request.hasAttempt()
                || request.getTaskKind() != TaskKind.TASK_KIND_CDC_INGEST) {
            throw new IllegalArgumentException(
                    "Worker-executed success requires a CDC ingest task attempt");
        }
        return TaskReport.newBuilder()
                .setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED)
                .setExecutionDrainState(
                        org.pangrova.protocol.v2.ExecutionDrainState
                                .EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED)
                .setCoverage(CoverageReport.newBuilder()
                        .setCompletionState(CompletionState.COMPLETION_STATE_COMPLETE)
                        .setExpectedWorkUnits(1)
                        .setCompletedWorkUnits(1)
                        .setCoverageRatio(1.0))
                .build().toByteArray();
    }

    /**
     * Builds an unsuccessful Worker-JVM receipt with no Native owner claim.
     * It is valid only for the CDC ingest task that was dispatched to this Worker.
     */
    public static byte[] workerExecutedFailure(
            TaskRequest request, TaskState state, ExecutionError failure) {
        if (request == null || request.getTaskKind() != TaskKind.TASK_KIND_CDC_INGEST
                || state == TaskState.TASK_STATE_FINISHED) {
            throw new IllegalArgumentException(
                    "Worker-executed failure requires an unsuccessful CDC ingest task");
        }
        return unsuccessfulReport(request, state, failure, null)
                .setExecutionDrainState(org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER)
                .build().toByteArray();
    }

    public static TaskReport unsuccessful(
            TaskRequest request, TaskState state, ErrorDetail error) {
        return unsuccessful(request, state, error, null);
    }

    /** Retains trustworthy Native usage while replacing uncertain failure coverage. */
    public static TaskReport unsuccessful(
            TaskRequest request, TaskState state, ErrorDetail error,
            TaskReport nativeReport) {
        if (request == null || !request.hasAttempt()) {
            throw new IllegalArgumentException("Worker terminal report needs a task attempt");
        }
        if (state != TaskState.TASK_STATE_FAILED
                && state != TaskState.TASK_STATE_CANCELLED) {
            throw new IllegalArgumentException("Worker can only report unsuccessful tasks");
        }
        if (error == null || error.errorCode() == null
                || error.errorCode().isBlank()) {
            throw new IllegalArgumentException("Worker terminal report needs a failure code");
        }
        return unsuccessfulWithError(request, state,
                workerError(state, error).build(), nativeReport);
    }

    /** Accepts already classified Worker failures without parsing REST strings. */
    public static TaskReport unsuccessfulWithError(
            TaskRequest request, TaskState state, ExecutionError failure, TaskReport nativeReport) {
        return unsuccessfulReport(request, state, failure, nativeReport).build();
    }

    private static TaskReport.Builder unsuccessfulReport(
            TaskRequest request, TaskState state, ExecutionError failure, TaskReport nativeReport) {
        if (request == null || !request.hasAttempt()
                || state != TaskState.TASK_STATE_FAILED && state != TaskState.TASK_STATE_CANCELLED) {
            throw new IllegalArgumentException("Worker failure needs an exact attempt and unsuccessful state");
        }
        if (failure == null || failure.getErrorClass() == ErrorClass.ERROR_CLASS_UNSPECIFIED
                || failure.getErrorClass() == ErrorClass.UNRECOGNIZED || failure.getCode().isBlank()
                || failure.getMessage().isBlank() || failure.getFailedComponent().isBlank()
                || !failure.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Worker failure needs a complete canonical classification");
        }
        CanonicalTaskRequests.validateCoverageIdentityBounds(request);
        var observedNative = matchingNativeReport(request, nativeReport);
        ExecutionError firstFailure = observedNative
                .filter(observed -> observed.getState() == state && observed.hasError()
                        && !observed.getError().getCode().isBlank())
                .map(TaskReport::getError).orElse(failure);
        var report = TaskReport.newBuilder()
                .setAttempt(request.getAttempt())
                .setState(state)
                .setReportOrigin(TaskReportOrigin
                        .TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .setCoverage(zeroCompletedCoverage(request))
                .setError(firstFailure);
        nativeUsage(request, observedNative).ifPresent(usage -> {
            report.setUsage(usage);
            observedNative.filter(TaskReport::hasNativeObjectReads)
                    .filter(CanonicalTaskRequests::nativeObjectReadsWithinUsage)
                    .ifPresent(observed -> report.setNativeObjectReads(observed.getNativeObjectReads()));
        });
        observedNative.filter(observed -> observed.getExecutionDrainState()
                == org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
                .ifPresent(observed -> {
                    if (observed.hasNativeTaskStats()) report.setNativeTaskStats(observed.getNativeTaskStats());
                    report.setExecutionDrainState(observed.getExecutionDrainState());
                });
        return report;
    }

    private static CoverageReport.Builder zeroCompletedCoverage(TaskRequest request) {
        var coverage = CoverageReport.newBuilder()
                .setCompletionState(CompletionState.COMPLETION_STATE_PARTIAL)
                .setExpectedWorkUnits(Math.max(1L, request.getSplitsCount()));
        Map<SourceKey, Long> sources = new LinkedHashMap<>();
        if (request.getSplitsCount() > MAX_EXPLICIT_FAILED_WORK_UNITS) {
            coverage.setAllWorkUnitsFailed(true);
        } else if (request.getSplitsCount() == 0) {
            coverage.addFailedWorkUnitIds(request.getAttempt().getTaskId());
        }
        for (var split : request.getSplitsList()) {
            if (!coverage.getAllWorkUnitsFailed()) {
                coverage.addFailedWorkUnitIds(split.getSplitId());
            }
            sources.merge(new SourceKey(split.getSourceId(), split.getSourceSnapshot()),
                    1L, Math::addExact);
        }
        sources.forEach((source, count) -> coverage.addSourceSnapshots(
                SourceSnapshotReport.newBuilder()
                        .setSourceId(source.id())
                        .setSnapshotId(source.snapshot())
                        .setExpectedWorkUnits(count)));
        appendBoundArtifacts(request, coverage);
        return coverage;
    }

    private static void appendBoundArtifacts(
            TaskRequest request, CoverageReport.Builder coverage) {
        // These are immutable bound identities, not a claim that execution
        // checked or consumed any artifact; completed work remains zero.
        for (var binding : request.getOperatorBindingsList()) {
            for (var artifact : binding.getArtifactsList()) {
                coverage.addArtifacts(ArtifactFreshnessReport.newBuilder()
                        .setArtifactId(artifact.getArtifactId())
                        .setArtifactType(artifact.getArtifactType())
                        .setGeneration(artifact.getGeneration())
                        .setSourceId(artifact.getSourceId())
                        .setSourceSnapshot(artifact.getSourceSnapshot())
                        .setSourceDataTimestampMs(artifact.getSourceDataTimestampMs())
                        .setArtifactCreatedAtEpochMs(
                                artifact.getArtifactCreatedAtEpochMs()));
            }
        }
    }

    private static ExecutionError.Builder workerError(
            TaskState state, ErrorDetail error) {
        String message = error.message() != null && !error.message().isBlank()
                ? error.message() : error.errorCode();
        if (message.length() > MAX_ERROR_MESSAGE_LENGTH) {
            message = message.substring(0, MAX_ERROR_MESSAGE_LENGTH);
        }
        return ExecutionError.newBuilder()
                .setErrorClass(state == TaskState.TASK_STATE_CANCELLED
                        ? ErrorClass.ERROR_CLASS_CANCELLED
                        : ErrorClass.ERROR_CLASS_INTERNAL)
                .setCode(error.errorCode())
                .setMessage(message)
                .setFailedComponent("worker");
    }

    private static Optional<ResourceUsage> nativeUsage(
            TaskRequest request, Optional<TaskReport> observed) {
        if (!request.hasResourceGrant()) {
            return Optional.empty();
        }
        return observed
                .filter(TaskReport::hasUsage)
                .map(TaskReport::getUsage)
                .filter(usage -> CanonicalTaskRequests.usageWithinGrant(usage, request.getResourceGrant()));
    }

    private static Optional<TaskReport> matchingNativeReport(TaskRequest request, TaskReport nativeStatus) {
        if (nativeStatus == null || nativeStatus.getReportOrigin() != TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                || !nativeStatus.getAttempt().equals(request.getAttempt())
                || !nativeStatus.getUnknownFields().asMap().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(nativeStatus);
    }

    private record SourceKey(String id, String snapshot) {}
}
