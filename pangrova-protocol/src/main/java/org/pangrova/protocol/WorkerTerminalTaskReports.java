package org.pangrova.protocol;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.v2.ArtifactFreshnessReport;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.SourceSnapshotReport;
import org.pangrova.protocol.v2.ResourceUsage;
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

    public static byte[] unsuccessful(
            TaskRequest request, TaskState state, ErrorDetail error) {
        return unsuccessful(request, state, error, null);
    }

    /** Retains trustworthy Native usage while replacing uncertain failure coverage. */
    public static byte[] unsuccessful(
            TaskRequest request, TaskState state, ErrorDetail error,
            byte[] nativeReport) {
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
        CanonicalTaskRequests.validateCoverageIdentityBounds(request);
        var report = TaskReport.newBuilder()
                .setAttempt(request.getAttempt())
                .setState(state)
                .setReportOrigin(TaskReportOrigin
                        .TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .setCoverage(zeroCompletedCoverage(request))
                .setError(terminalError(request, state, error, nativeReport));
        nativeUsage(request, nativeReport).ifPresent(report::setUsage);
        return report.build().toByteArray();
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

    private static ExecutionError.Builder terminalError(
            TaskRequest request, TaskState state, ErrorDetail error,
            byte[] nativeReport) {
        return matchingNativeReport(request, nativeReport)
                .filter(observed -> observed.getState() == state && observed.hasError()
                        && !observed.getError().getCode().isBlank())
                .map(observed -> observed.getError().toBuilder())
                .orElseGet(() -> workerError(state, error));
    }

    private static ExecutionError.Builder workerError(
            TaskState state, ErrorDetail error) {
        String message = error.message() != null ? error.message() : error.errorCode();
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
            TaskRequest request, byte[] encoded) {
        if (!request.hasResourceGrant()) {
            return Optional.empty();
        }
        return matchingNativeReport(request, encoded)
                .filter(TaskReport::hasUsage)
                .map(TaskReport::getUsage)
                .filter(usage -> CanonicalTaskRequests.usageWithinGrant(usage, request.getResourceGrant()));
    }

    private static Optional<TaskReport> matchingNativeReport(TaskRequest request, byte[] encoded) {
        if (encoded == null || encoded.length == 0) {
            return Optional.empty();
        }
        try {
            TaskReport nativeStatus = TaskReport.parseFrom(encoded);
            if (nativeStatus.getReportOrigin()
                    != TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                    || !nativeStatus.getAttempt().equals(request.getAttempt())) {
                return Optional.empty();
            }
            return Optional.of(nativeStatus);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            return Optional.empty();
        }
    }

    private record SourceKey(String id, String snapshot) {}
}
