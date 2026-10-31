package org.pangrova.coordinator.query.task.report;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactFreshnessReport;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExactFallbackReport;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.SourceSnapshotReport;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Validates the canonical v2 task report carried by the REST control channel. */
public final class CanonicalTaskReportValidator {

    private static final int MAX_STATUS_BYTES = 4 * 1024 * 1024;
    private static final int MAX_COVERAGE_ENTRIES =
            CanonicalTaskRequests.MAX_COVERAGE_IDENTITIES;
    private static final double COVERAGE_EPSILON = 1.0e-12;

    /** Validates an ordinary task report against its exact issued request and REST envelope. */
    public Optional<TaskReport> validate(
            TaskStatusReport report, SubmitTaskRequest expectedRequest) {
        validateOuterIdentity(report, expectedRequest);
        byte[] encoded = report.canonicalTaskReport();
        if (encoded == null || encoded.length == 0) {
            if (isTerminalState(report.state())) {
                throw invalid("terminal task is missing its canonical TaskReport");
            }
            return Optional.empty();
        }
        if (encoded.length > MAX_STATUS_BYTES) {
            throw invalid("TaskReport exceeds the control-plane size limit");
        }

        TaskReport status = parse(encoded);
        if (!status.getUnknownFields().asMap().isEmpty()) {
            throw invalid("TaskReport contains fields outside the negotiated descriptor");
        }
        validateAttempt(status, report, expectedRequest);
        validateState(status, report);
        validateOrigin(status);
        validateCoverage(status, expectedRequest);
        validateTaskUsage(status, expectedRequest);
        DataBuildTaskReportValidator.validate(status, report, expectedRequest);
        PhysicalWriteTaskReportValidator.validate(status, report, expectedRequest);
        return Optional.of(status);
    }

    /** Execution-owner receipt only: does not commit artifacts, elect winners or notify the DAG. */
    public TaskReport validateExecutionDrainReceipt(byte[] encoded, SubmitTaskRequest expected) {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_STATUS_BYTES) {
            throw invalid("Execution drain receipt exceeds the canonical control envelope");
        }
        TaskReport status = parse(encoded);
        CanonicalExecutionDrainReports.validateReceipt(status, expected);
        return status;
    }

    private static TaskReport parse(byte[] encoded) {
        try {
            TaskReport status = TaskReport.parseFrom(encoded);
            if (!status.hasAttempt()) {
                throw invalid("Native status is missing its fenced attempt identity");
            }
            return status;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Malformed canonical Native TaskReport", e);
        }
    }

    private static void validateOuterIdentity(
            TaskStatusReport report, SubmitTaskRequest expected) {
        if (expected == null) {
            return;
        }
        var canonical = expected.taskRequest();
        var attempt = canonical.getAttempt();
        if (!report.taskId().equals(attempt.getTaskId())
                || !report.queryId().equals(attempt.getQueryOrJobId())
                || report.stageId() != attempt.getStageId()
                || report.partitionId() != attempt.getPartitionId()
                || !report.taskType().equals(taskType(canonical))
                || report.attempt().planEpoch() != attempt.getPlanEpoch()
                || report.attempt().attemptId() != attempt.getAttemptId()
                || report.attempt().fencingToken() != attempt.getFencingToken()) {
            throw invalid("REST status identity differs from the dispatched request");
        }
    }

    private static void validateAttempt(
            TaskReport status, TaskStatusReport report,
            SubmitTaskRequest expected) {
        TaskAttemptId actual = status.getAttempt();
        var outer = report.attempt();
        if (!matchesOuterAttempt(actual, report, outer)) {
            throw invalid("Native status attempt differs from its REST envelope");
        }
        if (expected != null && !actual.getWorkerExecutionIdentity()
                .equals(expected.taskRequest().getAttempt().getWorkerExecutionIdentity())) {
            throw invalid("Native status Worker incarnation differs from the dispatched request");
        }
        long expectedControl = expected != null
                ? expected.taskRequest().getAttempt().getControlEpoch() : 0L;
        if (actual.getControlEpoch() != expectedControl) {
            throw invalid("Native status control epoch differs from the dispatched request");
        }
        var expectedOwner = expected != null
                ? expected.taskRequest().getAttempt().getExecutionOwner() : null;
        if (expectedOwner != null && !matchesExecutionOwner(actual, expectedOwner)) {
            throw invalid("Native status execution owner differs from the dispatched request");
        }
    }

    private static boolean matchesOuterAttempt(TaskAttemptId actual, TaskStatusReport report,
            org.pangrova.protocol.rest.task.TaskAttemptDescriptor outer) {
        return actual.getQueryOrJobId().equals(report.queryId())
                && actual.getTaskId().equals(report.taskId())
                && actual.getStageId() == report.stageId()
                && actual.getPartitionId() == report.partitionId()
                && actual.getPlanEpoch() == outer.planEpoch()
                && actual.getAttemptId() == outer.attemptId()
                && actual.getFencingToken() == outer.fencingToken();
    }

    private static boolean matchesExecutionOwner(TaskAttemptId actual,
            org.pangrova.protocol.v2.ExecutionOwnerIdentity expectedOwner) {
        return actual.hasExecutionOwner()
                && actual.getExecutionOwner().getOwnerId().equals(expectedOwner.getOwnerId())
                && actual.getExecutionOwner().getOwnerIncarnation()
                        .equals(expectedOwner.getOwnerIncarnation())
                && actual.getExecutionOwner().getFencingToken() == expectedOwner.getFencingToken();
    }

    private static void validateState(TaskReport status, TaskStatusReport report) {
        TaskState expected = switch (report.state()) {
            case "PENDING", "RECEIVED", "INITIALIZING", "SCHEDULED" ->
                    TaskState.TASK_STATE_PENDING;
            case "RUNNING" -> TaskState.TASK_STATE_RUNNING;
            case "FLUSHING", "FINISHING" -> TaskState.TASK_STATE_FLUSHING;
            case "FINISHED" -> TaskState.TASK_STATE_FINISHED;
            case "FAILED" -> TaskState.TASK_STATE_FAILED;
            case "CANCELLED" -> TaskState.TASK_STATE_CANCELLED;
            default -> throw invalid("Unknown REST task state: " + report.state());
        };
        if (status.getState() != expected) {
            throw invalid("Native status state differs from its REST envelope");
        }
        if (expected == TaskState.TASK_STATE_FAILED && !status.hasError()) {
            throw invalid("failed Native status is missing a structured error");
        }
        if (expected == TaskState.TASK_STATE_FINISHED && status.hasError()) {
            throw invalid("finished Native status contains an error");
        }
    }

    private static void validateOrigin(TaskReport status) {
        CanonicalExecutionDrainReports.validateOrigin(status);
    }

    static void validateCoverage(
            TaskReport status, SubmitTaskRequest expectedRequest) {
        if (!status.hasCoverage()) {
            throw invalid("TaskReport is missing coverage evidence");
        }
        ExpectedCoverage expected = expectedCoverage(expectedRequest);
        CoverageReport coverage = status.getCoverage();
        if (coverage.getExpectedWorkUnits() != expected.expectedWorkUnits()) {
            throw invalid("TaskReport coverage differs from dispatched work units");
        }
        if (coverage.getCompletedWorkUnits() > coverage.getExpectedWorkUnits()) {
            throw invalid("TaskReport completed work exceeds expected work");
        }
        double expectedRatio = (double) coverage.getCompletedWorkUnits()
                / (double) coverage.getExpectedWorkUnits();
        if (!Double.isFinite(coverage.getCoverageRatio())
                || Math.abs(coverage.getCoverageRatio() - expectedRatio)
                > COVERAGE_EPSILON) {
            throw invalid("TaskReport coverage ratio is inconsistent");
        }
        validateFailedWorkUnits(status, coverage, expected);
        validateSourceSnapshots(coverage, expected);
        validateArtifactFreshness(coverage, expected);
        validateCompletionState(status, coverage, expected);
    }

    static void validateTaskUsage(
            TaskReport status, SubmitTaskRequest expectedRequest) {
        if (!status.hasUsage()) {
            return;
        }
        if (expectedRequest == null
                || !expectedRequest.taskRequest().hasResourceGrant()) {
            throw invalid("TaskReport usage has no matching resource grant");
        }
        ResourceUsage usage = status.getUsage();
        StageResourceGrant grant = expectedRequest.taskRequest().getResourceGrant();
        if (!CanonicalTaskRequests.usageWithinGrant(usage, grant)) {
            throw invalid("TaskReport usage exceeds its dispatched resource grant");
        }
        if (status.getState() == TaskState.TASK_STATE_FINISHED
                && usage.getObservedProviderResponseBytes() != usage.getProviderResponseBytes()) {
            throw invalid("Finished TaskReport contains rejected provider response bytes");
        }
    }

    private static void validateFailedWorkUnits(
            TaskReport status, CoverageReport coverage, ExpectedCoverage expected) {
        if (coverage.getFailedWorkUnitIdsCount() > MAX_COVERAGE_ENTRIES) {
            throw invalid("TaskReport contains too many failed work-unit identities");
        }
        if (coverage.getAllWorkUnitsFailed()) {
            if ((status.getState() != TaskState.TASK_STATE_FAILED
                    && status.getState() != TaskState.TASK_STATE_CANCELLED)
                    || coverage.getCompletionState()
                    != CompletionState.COMPLETION_STATE_PARTIAL
                    || coverage.getCompletedWorkUnits() != 0
                    || coverage.getFailedWorkUnitIdsCount() != 0) {
                throw invalid("all-work-units-failed coverage is inconsistent");
            }
            return;
        }
        Set<String> failed = new HashSet<>();
        for (String workUnitId : coverage.getFailedWorkUnitIdsList()) {
            if (workUnitId.isBlank() || !failed.add(workUnitId)) {
                throw invalid("TaskReport failed work-unit identities are blank or duplicated");
            }
            if (!expected.splitIds().isEmpty()
                    && !expected.splitIds().contains(workUnitId)) {
                throw invalid("TaskReport names a work unit outside the dispatched request");
            }
        }
        long missing = coverage.getExpectedWorkUnits()
                - coverage.getCompletedWorkUnits();
        if (coverage.getCompletionState() == CompletionState.COMPLETION_STATE_PARTIAL
                && failed.size() != missing) {
            throw invalid("TaskReport partial coverage does not identify every missing work unit");
        }
    }

    private static void validateSourceSnapshots(
            CoverageReport coverage, ExpectedCoverage expected) {
        if (coverage.getSourceSnapshotsCount() > MAX_COVERAGE_ENTRIES) {
            throw invalid("TaskReport contains too many source snapshot entries");
        }
        Map<SourceKey, Long> actual = new HashMap<>();
        long completed = 0;
        for (SourceSnapshotReport source : coverage.getSourceSnapshotsList()) {
            SourceKey key = new SourceKey(source.getSourceId(), source.getSnapshotId());
            if (key.sourceId().isBlank() || key.snapshotId().isBlank()
                    || source.getExpectedWorkUnits() == 0
                    || source.getCompletedWorkUnits() > source.getExpectedWorkUnits()
                    || actual.putIfAbsent(key, source.getExpectedWorkUnits()) != null) {
                throw invalid("TaskReport contains invalid source snapshot coverage");
            }
            completed = Math.addExact(completed, source.getCompletedWorkUnits());
        }
        if (!actual.equals(expected.sourceWorkUnits())) {
            throw invalid("TaskReport source snapshots differ from dispatched splits");
        }
        if (!actual.isEmpty() && completed != coverage.getCompletedWorkUnits()) {
            throw invalid("TaskReport source completion differs from overall completion");
        }
    }

    private static void validateArtifactFreshness(
            CoverageReport coverage, ExpectedCoverage expected) {
        if (coverage.getArtifactsCount() > MAX_COVERAGE_ENTRIES) {
            throw invalid("TaskReport contains too many artifact freshness entries");
        }
        Set<PublishedGenerationKey> actual = new HashSet<>();
        for (ArtifactFreshnessReport report : coverage.getArtifactsList()) {
            PublishedGenerationKey key = requireArtifactKey(report, actual);
            ArtifactBinding binding = expected.artifacts().get(key);
            if (binding == null
                    || report.getSourceDataTimestampMs()
                    != binding.getSourceDataTimestampMs()
                    || report.getArtifactCreatedAtEpochMs()
                    != binding.getArtifactCreatedAtEpochMs()) {
                throw invalid("TaskReport artifact freshness differs from its binding");
            }
            if (!expected.sourceWorkUnits().isEmpty()
                    && !expected.sourceWorkUnits().containsKey(new SourceKey(
                    report.getSourceId(), report.getSourceSnapshot()))) {
                throw invalid("TaskReport artifact references an unpinned source snapshot");
            }
        }
        if (!actual.equals(expected.artifacts().keySet())) {
            throw invalid("TaskReport artifacts differ from dispatched bindings");
        }
    }

    private static PublishedGenerationKey requireArtifactKey(
            ArtifactFreshnessReport report, Set<PublishedGenerationKey> actual) {
        PublishedGenerationKey key = new PublishedGenerationKey(
                report.getArtifactId(), report.getArtifactType(),
                report.getGeneration(), report.getSourceId(),
                report.getSourceSnapshot());
        if (key.artifactId().isBlank() || key.artifactType().isBlank()
                || key.generation().isBlank() || key.sourceId().isBlank()
                || key.sourceSnapshot().isBlank()
                || report.getArtifactCreatedAtEpochMs() == 0
                || !actual.add(key)) {
            throw invalid("TaskReport contains invalid artifact freshness identity");
        }
        return key;
    }

    private static void validateCompletionState(
            TaskReport status, CoverageReport coverage, ExpectedCoverage expected) {
        CompletionState completion = coverage.getCompletionState();
        boolean allCompleted = coverage.getCompletedWorkUnits()
                == coverage.getExpectedWorkUnits();
        validateCompletionEvidence(status, coverage, expected, completion, allCompleted);

        if (status.getState() == TaskState.TASK_STATE_FINISHED
                && completion != CompletionState.COMPLETION_STATE_COMPLETE
                && completion != CompletionState.COMPLETION_STATE_FALLBACK_EXACT
                && completion != CompletionState.COMPLETION_STATE_PARTIAL) {
            throw invalid("finished TaskReport has no terminal completion state");
        }
        if ((status.getState() == TaskState.TASK_STATE_FAILED
                || status.getState() == TaskState.TASK_STATE_CANCELLED)
                && completion != CompletionState.COMPLETION_STATE_PARTIAL) {
            throw invalid("unsuccessful TaskReport must be PARTIAL");
        }
    }

    private static void validateCompletionEvidence(TaskReport status, CoverageReport coverage,
            ExpectedCoverage expected, CompletionState completion, boolean allCompleted) {
        switch (completion) {
            case COMPLETION_STATE_COMPLETE -> {
                if (!allCompleted || coverage.getFailedWorkUnitIdsCount() != 0
                        || coverage.getExactFallbacksCount() != 0) {
                    throw invalid("COMPLETE TaskReport contains incomplete or fallback work");
                }
            }
            case COMPLETION_STATE_PARTIAL -> {
                if (allCompleted || (status.getState() == TaskState.TASK_STATE_FINISHED
                        && !expected.explicitPartialAllowed())) {
                    throw invalid("PARTIAL TaskReport is not explicitly permitted");
                }
            }
            case COMPLETION_STATE_FALLBACK_EXACT -> {
                if (!allCompleted || coverage.getFailedWorkUnitIdsCount() != 0
                        || coverage.getExactFallbacksCount() == 0) {
                    throw invalid("FALLBACK_EXACT TaskReport lacks complete fallback evidence");
                }
                validateExactFallbacks(coverage, expected);
            }
            case COMPLETION_STATE_UNSPECIFIED -> {
                if (isTerminalState(status.getState())) {
                    throw invalid("terminal TaskReport has unspecified completion state");
                }
            }
            case UNRECOGNIZED -> throw invalid("TaskReport has unknown completion state");
            default -> throw invalid("TaskReport has unknown completion state");
        }
    }

    private static void validateExactFallbacks(
            CoverageReport coverage, ExpectedCoverage expected) {
        if (coverage.getExactFallbacksCount() > MAX_COVERAGE_ENTRIES) {
            throw invalid("TaskReport contains too many exact fallback entries");
        }
        Set<String> workUnits = new HashSet<>();
        for (ExactFallbackReport fallback : coverage.getExactFallbacksList()) {
            if (fallback.getWorkUnitId().isBlank()
                    || fallback.getCapabilityId().isBlank()
                    || fallback.getReasonCode().isBlank()
                    || fallback.getSourceId().isBlank()
                    || fallback.getSourceSnapshot().isBlank()
                    || !workUnits.add(fallback.getWorkUnitId())
                    || (!expected.splitIds().isEmpty()
                    && !expected.splitIds().contains(fallback.getWorkUnitId()))
                    || !expected.sourceWorkUnits().containsKey(new SourceKey(
                    fallback.getSourceId(), fallback.getSourceSnapshot()))
                    || !expected.exactFallbackCapabilities().contains(
                    fallback.getCapabilityId())) {
                throw invalid("TaskReport contains unbound exact fallback evidence");
            }
        }
    }

    private static ExpectedCoverage expectedCoverage(SubmitTaskRequest request) {
        if (request == null) {
            throw invalid("canonical TaskReport has no matching dispatched request");
        }
        Map<SourceKey, Long> sources = new LinkedHashMap<>();
        Set<String> splitIds = new HashSet<>();
        var canonical = request.taskRequest();
        for (DataSplit split : canonical.getSplitsList()) {
            try {
                DataSplitProtoCodec.fileDetail(split);
                String sourceId = split.getSourceId();
                if (split.getSplitId().isBlank() || !splitIds.add(split.getSplitId())
                        || sourceId.isBlank() || split.getSourceSnapshot().isBlank()) {
                    throw invalid("dispatched split has incomplete coverage identity");
                }
                sources.merge(new SourceKey(sourceId, split.getSourceSnapshot()), 1L,
                        Math::addExact);
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException(
                        "Invalid dispatched split coverage identity", failure);
            }
        }

        Map<PublishedGenerationKey, ArtifactBinding> artifacts = new LinkedHashMap<>();
        Set<String> exactFallbackCapabilities = new HashSet<>();
        boolean hasBindings = canonical.getOperatorBindingsCount() > 0;
        boolean explicitPartialAllowed = hasBindings;
        for (OperatorBinding binding : canonical.getOperatorBindingsList()) {
            explicitPartialAllowed &= binding.hasExecutionPolicy()
                    && binding.getExecutionPolicy().getCoveragePolicy()
                    == CoveragePolicy.COVERAGE_POLICY_ALLOW_EXPLICIT_PARTIAL;
            if (binding.hasCapability() && binding.hasExecutionPolicy()
                    && binding.getExecutionPolicy().getFallbackPolicy()
                    == FallbackPolicy.FALLBACK_POLICY_EXACT_SAME_SNAPSHOT) {
                exactFallbackCapabilities.add(binding.getCapability().getCapabilityId());
            }
            for (ArtifactBinding artifact : binding.getArtifactsList()) {
                PublishedGenerationKey key = new PublishedGenerationKey(
                        artifact.getArtifactId(), artifact.getArtifactType(),
                        artifact.getGeneration(), artifact.getSourceId(),
                        artifact.getSourceSnapshot());
                if (artifacts.putIfAbsent(key, artifact) != null) {
                    throw invalid("dispatched artifact binding identity is duplicated");
                }
            }
        }
        return new ExpectedCoverage(
                Math.max(1L, canonical.getSplitsCount()), Map.copyOf(sources),
                Set.copyOf(splitIds), Map.copyOf(artifacts), explicitPartialAllowed,
                Set.copyOf(exactFallbackCapabilities));
    }

    private static boolean isTerminalState(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state)
                || "CANCELLED".equals(state);
    }

    private static boolean isTerminalState(TaskState state) {
        return state == TaskState.TASK_STATE_FINISHED
                || state == TaskState.TASK_STATE_FAILED
                || state == TaskState.TASK_STATE_CANCELLED;
    }

    private record SourceKey(String sourceId, String snapshotId) {}

    private record PublishedGenerationKey(
            String artifactId, String artifactType, String generation,
            String sourceId, String sourceSnapshot) {}

    private record ExpectedCoverage(
            long expectedWorkUnits,
            Map<SourceKey, Long> sourceWorkUnits,
            Set<String> splitIds,
            Map<PublishedGenerationKey, ArtifactBinding> artifacts,
            boolean explicitPartialAllowed,
            Set<String> exactFallbackCapabilities) {}

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    static String taskType(org.pangrova.protocol.v2.TaskRequest request) {
        if (request.hasExecutionOptions()
                && request.getExecutionOptions().getDirectResultOutput()) {
            return "OUTPUT";
        }
        return switch (request.getTaskKind()) {
            case TASK_KIND_READ, TASK_KIND_SHUFFLE_READ, TASK_KIND_SHUFFLE_WRITE ->
                    "COMPUTE";
            case TASK_KIND_WRITE -> "WRITE";
            case TASK_KIND_BACKGROUND_BUILD -> "BACKGROUND_BUILD";
            case TASK_KIND_BACKGROUND_VALIDATE -> "BACKGROUND_VALIDATE";
            case TASK_KIND_BACKGROUND_EXPORT -> "BACKGROUND_EXPORT";
            default -> throw invalid("unknown canonical task kind");
        };
    }
}
