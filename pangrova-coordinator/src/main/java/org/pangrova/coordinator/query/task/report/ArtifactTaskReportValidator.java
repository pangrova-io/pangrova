package org.pangrova.coordinator.query.task.report;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactObjectScope;
import org.pangrova.protocol.v2.ArtifactWorkKind;
import org.pangrova.protocol.v2.ArtifactWorkReport;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;

/** Validates fenced artifact publication evidence in a canonical task report. */
final class ArtifactTaskReportValidator {

    private ArtifactTaskReportValidator() {}

    static void validate(
            TaskReport status, TaskStatusReport report,
            SubmitTaskRequest expected) {
        if (!status.hasArtifactWork()) {
            requireArtifactReportForSuccessfulBackgroundTask(status, expected);
            return;
        }
        if (!isExpectedFinishedArtifactTask(status, report, expected)) {
            throw invalid("artifact report is not attached to a known finished artifact task");
        }
        ArtifactWriteBinding output = outputBinding(
                expected.taskRequest().getOperatorBindingsList());
        ArtifactWorkReport work = status.getArtifactWork();
        validateTargetProvenance(output);
        validateReportIdentity(work, expected, output);
        validateReportChecksum(work);
        validateOutputs(work, output);
        validateUsage(work, expected.taskRequest().hasResourceGrant()
                ? expected.taskRequest().getResourceGrant() : null);
    }

    private static void requireArtifactReportForSuccessfulBackgroundTask(
            TaskReport status, SubmitTaskRequest expected) {
        if (expected != null && status.getState() == TaskState.TASK_STATE_FINISHED
                && isBackgroundTask(expected.taskRequest())) {
            throw invalid("finished background artifact task is missing its artifact report");
        }
    }

    private static boolean isExpectedFinishedArtifactTask(
            TaskReport status, TaskStatusReport report, SubmitTaskRequest expected) {
        return expected != null
                && CanonicalTaskReportValidator.taskType(expected.taskRequest())
                        .equals(report.taskType())
                && status.getState() == TaskState.TASK_STATE_FINISHED
                && isBackgroundTask(expected.taskRequest());
    }

    private static boolean isBackgroundTask(TaskRequest request) {
        return request.getTaskKind() == TaskKind.TASK_KIND_BACKGROUND_BUILD
                || request.getTaskKind() == TaskKind.TASK_KIND_BACKGROUND_EXPORT;
    }

    private static void validateTargetProvenance(ArtifactWriteBinding output) {
        if (output.getDependenciesCount() == 0
                || !output.hasDependencySetChecksum()
                || !isSha256(output.getDependencySetChecksum())
                || output.getEmptyPartitionPolicy()
                        == org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                .ARTIFACT_EMPTY_PARTITION_POLICY_UNSPECIFIED) {
            throw invalid("artifact target has incomplete dependency or empty-partition policy");
        }
        var dependencies = output.getDependenciesList().stream()
                .map(dependency -> new org.pangrova.indexing.api.ArtifactDependency(
                        dependency.getKind(), dependency.getId(), dependency.getVersion()))
                .toList();
        String checksum = java.util.HexFormat.of().formatHex(
                output.getDependencySetChecksum().getDigest().toByteArray());
        new org.pangrova.indexing.api.ArtifactDependencySet(dependencies, checksum);
    }

    private static ArtifactWriteBinding outputBinding(List<OperatorBinding> bindings) {
        List<ArtifactWriteBinding> outputs = bindings.stream()
                .filter(OperatorBinding::hasOutputArtifact)
                .map(OperatorBinding::getOutputArtifact)
                .toList();
        if (outputs.size() != 1) {
            throw invalid("background build must have exactly one artifact target");
        }
        return outputs.get(0);
    }

    private static void validateReportIdentity(
            ArtifactWorkReport work, SubmitTaskRequest expected,
            ArtifactWriteBinding output) {
        var attempt = expected.taskRequest().getAttempt();
        if (!matchesAttemptIdentity(work, attempt, output)
                || !matchesSourceIdentity(work, output)
                || !executionKindMatches(expected.taskRequest(), output)
                || work.hasError()) {
            throw invalid("artifact report identity or fence differs from its binding");
        }
    }

    private static boolean matchesAttemptIdentity(
            ArtifactWorkReport work, org.pangrova.protocol.v2.TaskAttemptId attempt,
            ArtifactWriteBinding output) {
        return work.getJobId().equals(attempt.getQueryOrJobId())
                && !output.getWorkUnitId().isBlank()
                && work.getPageId().equals(output.getWorkUnitId())
                && work.getFencingToken() == attempt.getFencingToken()
                && work.getFencingToken() == output.getPublicationFencingToken();
    }

    private static boolean matchesSourceIdentity(
            ArtifactWorkReport work, ArtifactWriteBinding output) {
        return work.getSourceId().equals(output.getSourceId())
                && work.getSourceSnapshot().equals(output.getSourceSnapshot())
                && work.getStagingGeneration().equals(output.getGeneration());
    }

    private static boolean executionKindMatches(
            org.pangrova.protocol.v2.TaskRequest request,
            ArtifactWriteBinding output) {
        return switch (output.getExecutionKind()) {
            case ARTIFACT_EXECUTION_KIND_BUILD -> request.getTaskKind()
                    == org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_BUILD;
            case ARTIFACT_EXECUTION_KIND_EXPORT -> request.getTaskKind()
                    == org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_EXPORT;
            case ARTIFACT_EXECUTION_KIND_UNSPECIFIED, UNRECOGNIZED -> false;
        };
    }

    private static void validateReportChecksum(ArtifactWorkReport work) {
        if (!isSha256(work.getReportChecksum())) {
            throw invalid("artifact report checksum is not SHA-256");
        }
        byte[] claimed = work.getReportChecksum().getDigest().toByteArray();
        byte[] unsigned = work.toBuilder().clearReportChecksum().build().toByteArray();
        if (!MessageDigest.isEqual(claimed, sha256(unsigned))) {
            throw invalid("artifact report checksum mismatch");
        }
    }

    private static void validateOutputs(
            ArtifactWorkReport work, ArtifactWriteBinding target) {
        if (work.getOutputsCount() == 0) {
            throw invalid("artifact report has no immutable outputs");
        }
        validateOutputTargetScope(target);
        Set<String> expectedSources = Set.copyOf(
                target.getExpectedSourceObjectIdsList());
        Set<String> actualSources = new HashSet<>();
        Set<String> objectIds = new HashSet<>();
        Set<String> locations = new HashSet<>();
        Set<Integer> ordinals = new HashSet<>();
        for (ArtifactObject object : work.getOutputsList()) {
            validateOutput(object, target, objectIds, locations, ordinals);
            if (object.getScope()
                    == ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT) {
                actualSources.add(object.getSourceObjectId());
            }
        }
        if (!actualSources.equals(expectedSources)) {
            throw invalid("artifact report source coverage differs from its work page");
        }
    }

    private static void validateOutputTargetScope(ArtifactWriteBinding target) {
        boolean valid = switch (target.getWorkKind()) {
            case ARTIFACT_WORK_KIND_SOURCE_MAP -> target.getOutputScopeId().isEmpty()
                    && target.getExpectedSourceObjectIdsCount() > 0;
            case ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE ->
                    target.getExpectedSourceObjectIdsCount() == 0
                    && target.getOutputScopeId().equals(target.getWorkUnitId());
            case ARTIFACT_WORK_KIND_GENERATION_FINALIZE ->
                    target.getExpectedSourceObjectIdsCount() == 0
                    && target.getOutputScopeId().equals(target.getGeneration());
            case ARTIFACT_WORK_KIND_RELATIONAL_PARTITION ->
                    target.getExpectedSourceObjectIdsCount() == 0
                    && target.getOutputScopeId().equals(target.getWorkUnitId())
                    && !target.getWorkUnitId().isBlank()
                    && !target.hasRowLocator()
                    && target.getArtifactType().equals("TABULAR_EXPORT")
                    && target.getFormat().getFormatId().equals("pangrova.tabular-export.columnar-stream")
                    && target.getExecutionKind() == org.pangrova.protocol.v2.ArtifactExecutionKind
                            .ARTIFACT_EXECUTION_KIND_EXPORT;
            case ARTIFACT_WORK_KIND_UNSPECIFIED, UNRECOGNIZED -> false;
        };
        if (!valid) {
            throw invalid("artifact target has an invalid work-kind scope identity");
        }
    }

    private static void validateOutput(
            ArtifactObject object, ArtifactWriteBinding target,
            Set<String> objectIds, Set<String> locations, Set<Integer> ordinals) {
        String prefix = target.getStagingUri().endsWith("/")
                ? target.getStagingUri() : target.getStagingUri() + "/";
        if (!hasValidOutputIdentity(object, prefix)
                || !hasValidOutputContent(object, target)
                || !objectIds.add(object.getObjectId())
                || !locations.add(object.getOpaqueUri())
                || !ordinals.add(object.getOrdinal())) {
            throw invalid("artifact report contains an invalid immutable output");
        }
        validateOutputScopeIdentity(object, target);
        if (target.getWorkKind() == ArtifactWorkKind.ARTIFACT_WORK_KIND_RELATIONAL_PARTITION
                && (!object.getObjectRole().equals("TABULAR_DATA") || object.getOrdinal() != 0
                        || object.getScope() != ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_WORK_UNIT)) {
            throw invalid("relational partition must publish the exact terminal TABULAR_DATA work-unit role");
        }
    }

    private static boolean hasValidOutputIdentity(
            ArtifactObject object, String stagingPrefix) {
        return !object.getObjectId().isBlank()
                && !object.getOpaqueUri().isBlank()
                && object.getOpaqueUri().startsWith(stagingPrefix)
                && !object.getObjectRole().isBlank()
                && !object.getSourceObjectId().isBlank();
    }

    private static boolean hasValidOutputContent(
            ArtifactObject object, ArtifactWriteBinding target) {
        return object.getSizeBytes() != 0
                && (object.getRowCount() != 0
                        || target.getEmptyPartitionPolicy()
                        == org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS)
                && isSha256(object.getChecksum())
                && object.getEncryptionKeyRef().equals(target.getEncryptionKeyRef());
    }

    private static void validateOutputScopeIdentity(
            ArtifactObject object, ArtifactWriteBinding target) {
        boolean valid = switch (object.getScope()) {
            case ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT ->
                    target.getWorkKind() == ArtifactWorkKind.ARTIFACT_WORK_KIND_SOURCE_MAP
                    && target.getExpectedSourceObjectIdsList()
                            .contains(object.getSourceObjectId());
            case ARTIFACT_OBJECT_SCOPE_WORK_UNIT ->
                    (target.getWorkKind() == ArtifactWorkKind.ARTIFACT_WORK_KIND_SOURCE_MAP
                    || target.getWorkKind()
                    == ArtifactWorkKind.ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE
                    || target.getWorkKind()
                    == ArtifactWorkKind.ARTIFACT_WORK_KIND_RELATIONAL_PARTITION)
                    && object.getSourceObjectId().equals(target.getWorkUnitId());
            case ARTIFACT_OBJECT_SCOPE_GENERATION ->
                    target.getWorkKind()
                    == ArtifactWorkKind.ARTIFACT_WORK_KIND_GENERATION_FINALIZE
                    && object.getSourceObjectId().equals(target.getGeneration());
            case ARTIFACT_OBJECT_SCOPE_UNSPECIFIED, UNRECOGNIZED -> false;
        };
        if (!valid) {
            throw invalid("artifact output scope identity differs from its work page");
        }
    }

    private static void validateUsage(
            ArtifactWorkReport work, StageResourceGrant grant) {
        if (grant == null) {
            throw invalid("artifact report has no matching resource grant");
        }
        var usage = work.getUsage();
        if (!CanonicalTaskRequests.usageWithinGrant(usage, grant)) {
            throw invalid("artifact report exceeds its dispatched resource grant");
        }
    }

    private static boolean isSha256(Checksum checksum) {
        return checksum.getAlgorithm() == Checksum.Algorithm.ALGORITHM_SHA256
                && checksum.getDigest().size() == 32;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }
}
