package org.pangrova.coordinator.query.task.report;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.task.WorkerTerminalTaskReports;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObjectScope;
import org.pangrova.protocol.v2.ArtifactWorkReport;
import org.pangrova.protocol.v2.ArtifactWorkKind;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.SourceSnapshotReport;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalTaskReportValidatorTest {

    private static final TaskAttemptDescriptor ATTEMPT = TaskAttemptDescriptor.of(4, 2);
    private static final long OWNER_EPOCH = 17;
    private static final ExecutionOwnerIdentity OWNER =
            new ExecutionOwnerIdentity("coordinator-a", "process-a", OWNER_EPOCH);

    private final CanonicalTaskReportValidator validator =
            new CanonicalTaskReportValidator();

    @Test
    void acceptsExactFencedNativeTerminalStatus() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .build();

        var validated = validator.validate(report(request, status), request);

        assertThat(validated).contains(status);
    }

    @Test
    void rejectsAReportFromAnotherPhysicalWorkerIncarnation() {
        var physical = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
        var task = request("COMPUTE", List.of()).taskRequest().toBuilder();
        task.getAttemptBuilder().setWorkerExecutionIdentity(physical.toProto());
        var expected = SubmitTaskRequest.from(task.build());
        var status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(task.getAttempt().toBuilder().setWorkerExecutionIdentity(
                        physical.toProto().toBuilder()
                                .setIncarnationId("00000000-0000-0000-0000-0000000000e2")))
                .setState(TaskState.TASK_STATE_FINISHED).setCoverage(completeCoverage()).build();

        assertThatThrownBy(() -> validator.validate(report(expected, status), expected))
                .hasMessageContaining("Worker incarnation differs");
    }

    @Test
    void rejectsTerminalReportWithoutExplicitOrigin() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .clearReportOrigin()
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("report origin");
    }

    @Test
    void rejectsWorkerReconciledSuccess() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot claim success");
    }

    @Test
    void acceptsFencedWorkerFailureWithZeroCompletedWork() throws Exception {
        var split = DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "iceberg", "catalog.schema.table", "snapshot-9", "object-9",
                        "s3://bucket/data.parquet", 0, 100, Map.of(),
                        FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET).build()));
        SubmitTaskRequest request = request("COMPUTE", List.of(), List.of(split));
        byte[] encoded = WorkerTerminalTaskReports.unsuccessful(
                request.taskRequest(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("WORKER_SETUP_FAILED", "IllegalStateException",
                        "Native allocation failed"));
        TaskReport status = TaskReport.parseFrom(encoded);

        assertThat(status.getCoverage().getCompletedWorkUnits()).isZero();
        assertThat(status.getCoverage().getFailedWorkUnitIdsList())
                .containsExactly(split.getSplitId());
        assertThat(status.getCoverage().getSourceSnapshots(0).getCompletedWorkUnits())
                .isZero();
        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
    }

    @Test
    void acceptsAggregateFailureCoverageForMoreThanFourThousandSplits()
            throws Exception {
        var fileSplit = DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "iceberg", "catalog.schema.table", "snapshot-9", "object-9",
                        "s3://bucket/data.parquet", 0, 100, Map.of(),
                        FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET).build()));
        var splits = IntStream.range(0, 4_097)
                .mapToObj(index -> fileSplit.toBuilder()
                        .setSplitId("split-" + index).build())
                .toList();
        SubmitTaskRequest request = request("COMPUTE", List.of(), splits);
        TaskReport status = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.taskRequest(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("SOURCE_FAILED", "Failure", "source read failed")));

        assertThat(status.getCoverage().getAllWorkUnitsFailed()).isTrue();
        assertThat(status.getCoverage().getFailedWorkUnitIdsCount()).isZero();
        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
    }

    @Test
    void acceptsFencedWorkerCancellationWithoutNativeReport() throws Exception {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.taskRequest(), TaskState.TASK_STATE_CANCELLED,
                new ErrorDetail("TASK_CANCELLED", "Cancellation", "Cancelled")));

        assertThat(status.getError().getErrorClass())
                .isEqualTo(ErrorClass.ERROR_CLASS_CANCELLED);
        assertThat(validator.validate(report(request, status, "CANCELLED"), request))
                .contains(status);
    }

    @Test
    void acceptsFencedWorkerWriteFailure() throws Exception {
        SubmitTaskRequest request = request("WRITE", List.of());
        TaskReport status = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.taskRequest(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("WRITE_BIND_FAILED", "IllegalStateException",
                        "Connector writer unavailable")));

        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
        assertThat(status.getAttempt()).isEqualTo(request.taskRequest().getAttempt());
    }

    @Test
    void workerFailureRetainsFencedNativeResourceEvidence() throws Exception {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        ResourceUsage observedUsage = ResourceUsage.newBuilder()
                .setPeakNativeMemoryBytes(1024)
                .setProviderRequestBytes(7)
                .build();
        byte[] nativeFailure = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(request.taskRequest().getAttempt())
                .setState(TaskState.TASK_STATE_FAILED)
                .setUsage(observedUsage)
                .setError(ExecutionError.newBuilder()
                        .setErrorClass(ErrorClass.ERROR_CLASS_PROVIDER)
                        .setCode("PROVIDER_FAILED")
                        .setMessage("provider rejected request")
                        .setFailedComponent("provider"))
                .build().toByteArray();
        TaskReport status = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.taskRequest(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("PROVIDER_FAILED", "ERROR_CLASS_PROVIDER",
                        "provider rejected request"), nativeFailure));

        assertThat(status.getUsage()).isEqualTo(observedUsage);
        assertThat(status.getError().getCode()).isEqualTo("PROVIDER_FAILED");
        assertThat(status.getError().getFailedComponent()).isEqualTo("provider");
        assertThat(status.getReportOrigin())
                .isEqualTo(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
    }

    @Test
    void workerFailureRetainsBoundArtifactIdentityWithoutClaimingCompletedWork()
            throws Exception {
        ArtifactBinding artifact = ArtifactBinding.newBuilder()
                .setArtifactId("index-1")
                .setArtifactType("vector-index")
                .setGeneration("generation-1")
                .setSourceId("catalog.schema.table")
                .setSourceSnapshot("snapshot-9")
                .setSourceDataTimestampMs(100)
                .setArtifactCreatedAtEpochMs(101)
                .build();
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("scan-1")
                .addArtifacts(artifact)
                .build();
        SubmitTaskRequest request = request("COMPUTE", List.of(binding));
        TaskReport status = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.taskRequest(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("WORKER_SETUP_FAILED", "IllegalStateException",
                        "Native allocation failed")));

        assertThat(status.getCoverage().getCompletedWorkUnits()).isZero();
        assertThat(status.getCoverage().getArtifacts(0).getArtifactId())
                .isEqualTo(artifact.getArtifactId());
        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
    }

    @Test
    void rejectsSuccessfulBackgroundBuildWithoutArtifactReport() {
        assertMissingSuccessfulArtifactReportRejected("BACKGROUND_BUILD");
    }

    @Test
    void rejectsSuccessfulBackgroundExportWithoutArtifactReport() {
        assertMissingSuccessfulArtifactReportRejected("BACKGROUND_EXPORT");
    }

    @Test
    void acceptsFailedBackgroundBuildWithoutArtifactReport() {
        SubmitTaskRequest request = request("BACKGROUND_BUILD", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FAILED)
                .setCoverage(CoverageReport.newBuilder()
                        .setCompletionState(CompletionState.COMPLETION_STATE_PARTIAL)
                        .setExpectedWorkUnits(1)
                        .addFailedWorkUnitIds("task-1"))
                .setError(ExecutionError.newBuilder()
                        .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL)
                        .setCode("NATIVE_TASK_FAILED")
                        .setMessage("artifact build failed"))
                .build();

        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
    }

    private void assertMissingSuccessfulArtifactReportRejected(String taskType) {
        SubmitTaskRequest request = request(taskType, List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing its artifact report");
    }

    @Test
    void rejectsNativeIdentityThatDiffersFromRestEnvelope() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt().toBuilder().setTaskId("forged-task"))
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt differs");
    }

    @Test
    void rejectsOuterIdentityThatDiffersFromDispatchedRequest() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .build();
        TaskStatusReport forged = new TaskStatusReport(
                "task-1", "other-query", 3, 7, "COMPUTE", "worker-1",
                "FINISHED", null, null, null, 1, 1, null, null,
                ATTEMPT, status.toByteArray());

        assertThatThrownBy(() -> validator.validate(forged, request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dispatched request");
    }

    @Test
    void acceptsChecksummedBoundArtifactMetadataWithoutArtifactBytes() {
        ArtifactWriteBinding target = artifactTarget();
        OperatorBinding model = OperatorBinding.newBuilder()
                .setPlanNodeId("model-invocation")
                .setModel(ModelBinding.newBuilder().setServiceId("model-service"))
                .build();
        OperatorBinding sink = OperatorBinding.newBuilder()
                .setPlanNodeId("artifact-build")
                .setOutputArtifact(target)
                .build();
        SubmitTaskRequest request = request(
                "BACKGROUND_BUILD",
                List.of(model, sink));
        ArtifactWorkReport work = signedReport(target);
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setArtifactWork(work)
                .build();

        var validated = validator.validate(report(request, status), request);

        assertThat(validated.orElseThrow().getArtifactWork()).isEqualTo(work);
    }

    @Test
    void acceptsWorkUnitScopedReduceOutput() {
        assertArtifactOutputAccepted(artifactTarget(
                ArtifactWorkKind.ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE));
    }

    @Test
    void acceptsGenerationScopedFinalizeOutput() {
        assertArtifactOutputAccepted(artifactTarget(
                ArtifactWorkKind.ARTIFACT_WORK_KIND_GENERATION_FINALIZE));
    }

    @Test
    void acceptsExactRelationalTerminalWorkUnitWithoutPhysicalLocatorOrSourceCoverage() {
        assertArtifactOutputAccepted(artifactTarget(ArtifactWorkKind.ARTIFACT_WORK_KIND_RELATIONAL_PARTITION));
    }

    @Test
    void rejectsRelationalTerminalPhysicalLocatorSourceCoverageAndScopeDrift() {
        var target = artifactTarget(ArtifactWorkKind.ARTIFACT_WORK_KIND_RELATIONAL_PARTITION);
        for (var invalid : List.of(target.toBuilder().addExpectedSourceObjectIds("physical-source").build(),
                target.toBuilder().setOutputScopeId("another-page").build(),
                target.toBuilder().setRowLocator(org.pangrova.protocol.v2.RowLocatorContract
                        .getDefaultInstance()).build())) {
            assertThatThrownBy(() -> assertArtifactOutputAccepted(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("invalid work-kind scope identity");
        }
    }

    @Test
    void rejectsMoreThanOneArtifactTargetAcrossBindings() {
        ArtifactWriteBinding target = artifactTarget();
        SubmitTaskRequest request = request("BACKGROUND_BUILD", List.of(
                OperatorBinding.newBuilder().setPlanNodeId("sink-a")
                        .setOutputArtifact(target).build(),
                OperatorBinding.newBuilder().setPlanNodeId("sink-b")
                        .setOutputArtifact(target).build()));
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setArtifactWork(signedReport(target))
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one artifact target");
    }

    @Test
    void rejectsForgedArtifactReportChecksum() {
        ArtifactWriteBinding target = artifactTarget();
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("artifact-build")
                .setOutputArtifact(target)
                .build();
        SubmitTaskRequest request = request(
                "BACKGROUND_BUILD",
                List.of(binding));
        ArtifactWorkReport signed = signedReport(target);
        ArtifactWorkReport forged = signed.toBuilder()
                .setOutputs(0, signed.getOutputs(0).toBuilder()
                        .setObjectRole("TAMPERED"))
                .build();
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setArtifactWork(forged)
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checksum mismatch");
    }

    @Test
    void rejectsArtifactMetadataFromForegroundTask() {
        ArtifactWriteBinding target = artifactTarget();
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setArtifactWork(signedReport(target))
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("known finished artifact task");
    }

    @Test
    void rejectsTerminalReportWithoutCanonicalTaskReport() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskStatusReport report = new TaskStatusReport(
                request.taskRequest().getAttempt().getTaskId(),
                request.taskRequest().getAttempt().getQueryOrJobId(),
                request.taskRequest().getAttempt().getStageId(),
                request.taskRequest().getAttempt().getPartitionId(),
                taskType(request), "worker-1", "FINISHED",
                null, null, null, 1, 1, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), null);

        assertThatThrownBy(() -> validator.validate(report, request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing its canonical TaskReport");
    }

    @Test
    void rejectsSilentPartialSuccessUnderCompleteByDefaultPolicy() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(CoverageReport.newBuilder()
                        .setCompletionState(CompletionState.COMPLETION_STATE_PARTIAL)
                        .setExpectedWorkUnits(1)
                        .addFailedWorkUnitIds("task-1"))
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not explicitly permitted");
    }

    @Test
    void acceptsTruthfulPartialCoverageForFailedAttempt() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FAILED)
                .setCoverage(CoverageReport.newBuilder()
                        .setCompletionState(CompletionState.COMPLETION_STATE_PARTIAL)
                        .setExpectedWorkUnits(1)
                        .addFailedWorkUnitIds("task-1"))
                .setError(ExecutionError.newBuilder()
                        .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL)
                        .setCode("NATIVE_TASK_FAILED")
                        .setMessage("boom"))
                .build();

        assertThat(validator.validate(report(request, status, "FAILED"), request))
                .contains(status);
    }

    @Test
    void rejectsProviderUsageAboveDispatchedTaskGrant() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setUsage(ResourceUsage.newBuilder()
                        .setProviderRequestBytes(101))
                .build();

        assertThatThrownBy(() -> validator.validate(report(request, status), request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resource grant");
    }

    @ParameterizedTest
    @ValueSource(strings = {"cpu_nanos", "peak_native_memory_bytes", "peak_jvm_heap_bytes", "spill_bytes",
            "shuffle_send_bytes", "shuffle_receive_bytes", "object_store_read_bytes", "object_store_write_bytes",
            "candidate_rows", "candidate_bytes", "provider_items", "provider_attempts",
            "provider_request_bytes", "provider_response_bytes", "observed_provider_response_bytes", "media_bytes"})
    void rejectsUnrepresentableUnsignedTaskAndArtifactUsage(String fieldName) {
        var usage = ResourceUsage.newBuilder();
        usage.setField(usage.getDescriptorForType().findFieldByName(fieldName), -1L);
        SubmitTaskRequest ordinary = request("COMPUTE", List.of());
        var status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt()).setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage()).setUsage(usage).build();
        assertThatThrownBy(() -> validator.validate(report(ordinary, status), ordinary))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("resource grant");

        var target = artifactTarget();
        var sink = OperatorBinding.newBuilder().setPlanNodeId("artifact-build").setOutputArtifact(target).build();
        SubmitTaskRequest artifact = request("BACKGROUND_BUILD", List.of(sink));
        var unsigned = signedReport(target).toBuilder().clearReportChecksum().setUsage(usage).build();
        var signed = unsigned.toBuilder().setReportChecksum(sha256Checksum(unsigned.toByteArray())).build();
        var artifactStatus = status.toBuilder().clearUsage().setArtifactWork(signed).build();
        assertThatThrownBy(() -> validator.validate(report(artifact, artifactStatus), artifact))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("resource grant");
    }

    @Test
    void acceptsExactFailureWithObservedRejectedChunkWithoutDroppingResourceEvidence() throws Exception {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        var usage = ResourceUsage.newBuilder().setProviderAttempts(1).setProviderResponseBytes(50)
                .setObservedProviderResponseBytes(1001).build();
        byte[] nativeFailure = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(request.taskRequest().getAttempt()).setState(TaskState.TASK_STATE_FAILED)
                .setUsage(usage).setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_PROVIDER)
                        .setCode("AI_BUDGET_EXCEEDED").setMessage("response retention denied")).build().toByteArray();
        TaskReport failed = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(request.taskRequest(),
                TaskState.TASK_STATE_FAILED, new ErrorDetail("AI_BUDGET_EXCEEDED", "AiInvocationException",
                        "response retention denied"), nativeFailure));
        assertThat(failed.getUsage()).isEqualTo(usage);
        assertThat(validator.validate(report(request, failed, "FAILED"), request)).contains(failed);
    }

    @Test
    void rejectedResponseObservationCannotClaimSuccessfulCompletion() {
        SubmitTaskRequest request = request("COMPUTE", List.of());
        var finished = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt()).setState(TaskState.TASK_STATE_FINISHED).setCoverage(completeCoverage())
                .setUsage(ResourceUsage.newBuilder().setProviderResponseBytes(50)
                        .setObservedProviderResponseBytes(51)).build();
        assertThatThrownBy(() -> validator.validate(report(request, finished), request))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rejected provider response");
    }

    @Test
    void requiresSourceSnapshotCoverageToMatchDispatchedSplits() {
        var split = DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                "iceberg", "catalog.schema.table", "snapshot-9", "object-9",
                "s3://bucket/data.parquet",
                0, 100, Map.of(), FileSplitDetail.newBuilder()
                        .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                        .build()));
        SubmitTaskRequest request = request(
                "COMPUTE", List.of(), List.of(split));
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage().toBuilder()
                        .addSourceSnapshots(SourceSnapshotReport.newBuilder()
                                .setSourceId("catalog.schema.table")
                                .setSnapshotId("snapshot-9")
                                .setExpectedWorkUnits(1)
                                .setCompletedWorkUnits(1)))
                .build();

        assertThat(validator.validate(report(request, status), request)).contains(status);
    }

    private static SubmitTaskRequest request(
            String taskType, List<OperatorBinding> bindings) {
        return request(taskType, bindings, List.of());
    }

    private static SubmitTaskRequest request(
            String taskType, List<OperatorBinding> bindings,
            List<org.pangrova.protocol.v2.DataSplit> splits) {
        byte[] plan = {1};
        TaskKind kind = switch (taskType) {
            case "BACKGROUND_BUILD" -> TaskKind.TASK_KIND_BACKGROUND_BUILD;
            case "BACKGROUND_EXPORT" -> TaskKind.TASK_KIND_BACKGROUND_EXPORT;
            case "WRITE" -> TaskKind.TASK_KIND_WRITE;
            default -> TaskKind.TASK_KIND_READ;
        };
        TaskRequest canonical = TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(nativeAttempt())
                .setTaskKind(kind)
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId("tenant").setPrincipal("test-user"))
                .setSubstraitPlan(ByteString.copyFrom(plan))
                .setSubstraitPlanChecksum(ProtocolChecksums.sha256(plan))
                .addAllSplits(splits)
                .addAllOperatorBindings(bindings)
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId("task-1:grant")
                        .setNativeMemoryBytes(4096)
                        .setCpuSlots(1)
                        .setSpillBytes(4096)
                        .setProviderConcurrency(2)
                        .setProviderItems(100)
                        .setProviderAttempts(110)
                        .setProviderRequestBytes(100)
                        .setProviderResponseBytes(50)
                        .setMediaBytes(1024))
                .setDeadlineEpochMs(4_102_444_800_000L)
                .build();
        return SubmitTaskRequest.from(canonical);
    }

    private static TaskStatusReport report(
            SubmitTaskRequest request, TaskReport status) {
        return report(request, status, "FINISHED");
    }

    private static TaskStatusReport report(
            SubmitTaskRequest request, TaskReport status, String state) {
        return new TaskStatusReport(
                request.taskRequest().getAttempt().getTaskId(),
                request.taskRequest().getAttempt().getQueryOrJobId(),
                request.taskRequest().getAttempt().getStageId(),
                request.taskRequest().getAttempt().getPartitionId(),
                taskType(request), "worker-1", state,
                null, null, null, 1, 1, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()),
                status.toByteArray());
    }

    private static String taskType(SubmitTaskRequest request) {
        return switch (request.taskRequest().getTaskKind()) {
            case TASK_KIND_BACKGROUND_BUILD -> "BACKGROUND_BUILD";
            case TASK_KIND_BACKGROUND_EXPORT -> "BACKGROUND_EXPORT";
            case TASK_KIND_WRITE -> "WRITE";
            default -> "COMPUTE";
        };
    }

    private static TaskAttemptId nativeAttempt() {
        return TaskAttemptId.newBuilder()
                .setQueryOrJobId("job-1")
                .setTaskId("task-1")
                .setStageId(3)
                .setPartitionId(7)
                .setPlanEpoch(ATTEMPT.planEpoch())
                .setAttemptId(ATTEMPT.attemptId())
                .setFencingToken(ATTEMPT.fencingToken())
                .setControlEpoch(OWNER_EPOCH)
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity
                        .newBuilder()
                        .setOwnerId(OWNER.ownerId())
                        .setOwnerIncarnation(OWNER.ownerIncarnation())
                        .setFencingToken(OWNER.fencingToken()))
                .build();
    }

    private static CoverageReport completeCoverage() {
        return CoverageReport.newBuilder()
                .setCompletionState(CompletionState.COMPLETION_STATE_COMPLETE)
                .setExpectedWorkUnits(1)
                .setCompletedWorkUnits(1)
                .setCoverageRatio(1.0)
                .build();
    }

    private static ArtifactWriteBinding artifactTarget() {
        return artifactTarget(ArtifactWorkKind.ARTIFACT_WORK_KIND_SOURCE_MAP);
    }

    private static ArtifactWriteBinding artifactTarget(ArtifactWorkKind workKind) {
        ArtifactWriteBinding.Builder target = ArtifactWriteBinding.newBuilder()
                .setArtifactId("index-1")
                .setArtifactType("VECTOR_INDEX")
                .setGeneration("generation-8")
                .setSourceId("catalog.schema.table")
                .setSourceSnapshot("snapshot-8")
                .setStagingUri("s3://bucket/staging/job-1")
                .setPublicationFencingToken(ATTEMPT.fencingToken())
                .setWorkUnitId("page-1")
                .setIdempotencyKey("job-1:task-1")
                .setWorkKind(workKind)
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_BUILD)
                .addDependencies(org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT")
                        .setId("catalog.schema.table")
                        .setVersion("snapshot-8"))
                .setDependencySetChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                java.util.HexFormat.of().parseHex(
                                        org.pangrova.generation.api.GenerationDependencySet
                                                .sourceSnapshot("catalog.schema.table",
                                                        "snapshot-8").sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2
                        .ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID);
        return switch (workKind) {
            case ARTIFACT_WORK_KIND_SOURCE_MAP -> target
                    .addExpectedSourceObjectIds("source-1").build();
            case ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE -> target
                    .setOutputScopeId("page-1").build();
            case ARTIFACT_WORK_KIND_GENERATION_FINALIZE -> target
                    .setOutputScopeId("generation-8").build();
            case ARTIFACT_WORK_KIND_RELATIONAL_PARTITION -> target
                    .setArtifactType("TABULAR_EXPORT")
                    .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind.ARTIFACT_EXECUTION_KIND_EXPORT)
                    .setFormat(org.pangrova.protocol.v2.ArtifactFormatRef.newBuilder()
                            .setFormatId("pangrova.tabular-export.arrow-stream"))
                    .setOutputScopeId("page-1").build();
            default -> throw new IllegalArgumentException("unsupported test work kind");
        };
    }

    private static ArtifactWorkReport signedReport(ArtifactWriteBinding target) {
        ArtifactObjectScope scope;
        String scopeId;
        switch (target.getWorkKind()) {
            case ARTIFACT_WORK_KIND_SOURCE_MAP -> {
                scope = ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT;
                scopeId = "source-1";
            }
            case ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE, ARTIFACT_WORK_KIND_RELATIONAL_PARTITION -> {
                scope = ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_WORK_UNIT;
                scopeId = target.getWorkUnitId();
            }
            case ARTIFACT_WORK_KIND_GENERATION_FINALIZE -> {
                scope = ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_GENERATION;
                scopeId = target.getGeneration();
            }
            default -> throw new IllegalArgumentException("unsupported test work kind");
        }
        ArtifactObject object = ArtifactObject.newBuilder()
                .setObjectId("graph-abc")
                .setOpaqueUri(target.getStagingUri() + "/graph-abc.bin")
                .setSizeBytes(128)
                .setChecksum(sha256Checksum(new byte[]{1, 2, 3}))
                .setObjectRole(target.getWorkKind() == ArtifactWorkKind.ARTIFACT_WORK_KIND_RELATIONAL_PARTITION
                        ? "TABULAR_DATA" : "GRAPH")
                .setSourceObjectId(scopeId)
                .setRowCount(4)
                .setOrdinal(0)
                .setScope(scope)
                .build();
        ArtifactWorkReport unsigned = ArtifactWorkReport.newBuilder()
                .setJobId("job-1")
                .setPageId(target.getWorkUnitId())
                .setFencingToken(ATTEMPT.fencingToken())
                .setSourceId(target.getSourceId())
                .setSourceSnapshot(target.getSourceSnapshot())
                .setStagingGeneration(target.getGeneration())
                .addOutputs(object)
                .build();
        return unsigned.toBuilder()
                .setReportChecksum(sha256Checksum(unsigned.toByteArray()))
                .build();
    }

    private void assertArtifactOutputAccepted(ArtifactWriteBinding target) {
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("artifact-build")
                .setOutputArtifact(target)
                .build();
        SubmitTaskRequest request = request(target.getExecutionKind()
                == org.pangrova.protocol.v2.ArtifactExecutionKind.ARTIFACT_EXECUTION_KIND_EXPORT
                ? "BACKGROUND_EXPORT" : "BACKGROUND_BUILD", List.of(binding));
        ArtifactWorkReport work = signedReport(target);
        TaskReport status = TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(completeCoverage())
                .setArtifactWork(work)
                .build();

        assertThat(validator.validate(report(request, status), request))
                .contains(status);
    }

    private static Checksum sha256Checksum(byte[] value) {
        try {
            return Checksum.newBuilder()
                    .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                    .setDigest(ByteString.copyFrom(
                            MessageDigest.getInstance("SHA-256").digest(value)))
                    .build();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
