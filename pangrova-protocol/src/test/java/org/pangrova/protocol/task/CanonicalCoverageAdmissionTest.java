package org.pangrova.protocol.task;

import org.pangrova.protocol.CanonicalTaskRequestTestFixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

class CanonicalCoverageAdmissionTest {

    @Test
    void acceptsBoundaryAndReconcilesLargeSplitSetWithoutTruncatingCoverage()
            throws Exception {
        var request = request();
        for (int index = 0; index < 4_097; index++) {
            request.addSplits(split(index, "catalog.schema.events"));
        }

        CanonicalTaskRequests.validateForAdmission(request.build());
        var report = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.build(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("TASK_FAILED", "Failure", "source read failed")));

        assertThat(report.getCoverage().getAllWorkUnitsFailed()).isTrue();
        assertThat(report.getCoverage().getFailedWorkUnitIdsCount()).isZero();
        assertThat(report.getCoverage().getExpectedWorkUnits()).isEqualTo(4_097);
        assertThat(report.getCoverage().getSourceSnapshotsCount()).isEqualTo(1);
    }

    @Test
    void rejectsUnreportableDistinctSourceIdentitiesBeforeWorkerAdmission() {
        var request = request();
        for (int index = 0; index < 4_096; index++) {
            request.addSplits(split(index, "catalog.schema.events-" + index));
        }
        CanonicalTaskRequests.validateForAdmission(request.build());
        request.addSplits(split(4_096, "catalog.schema.events-4096"));

        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(
                request.build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too many source snapshot coverage identities");
        assertThatThrownBy(() -> WorkerTerminalTaskReports.unsuccessful(
                request.build(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("TASK_FAILED", "Failure", "source read failed")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too many source snapshot coverage identities");
    }

    @Test
    void rejectsUnreportableArtifactIdentitiesBeforeWorkerAdmission() {
        var request = request();
        var binding = OperatorBinding.newBuilder().setPlanNodeId("scan-1");
        for (int index = 0; index < 4_096; index++) {
            binding.addArtifacts(ArtifactBinding.newBuilder()
                    .setArtifactId("index-" + index));
        }
        request.addOperatorBindings(binding);
        CanonicalTaskRequests.validateForAdmission(request.build());
        request.getOperatorBindingsBuilder(0).addArtifacts(
                ArtifactBinding.newBuilder().setArtifactId("index-4096"));

        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(
                request.build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too many artifact coverage identities");
    }

    @Test
    void doesNotAttributeUnverifiedUsageToNative() throws Exception {
        var request = request()
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setNativeMemoryBytes(4_096));
        var foreignReport = TaskReport.newBuilder()
                .setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FAILED)
                .setReportOrigin(TaskReportOrigin
                        .TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .setUsage(ResourceUsage.newBuilder().setPeakNativeMemoryBytes(1))
                .build();

        var reconciled = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.build(), TaskState.TASK_STATE_FAILED,
                new ErrorDetail("TASK_FAILED", "Failure", "source read failed"),
                foreignReport.toByteArray()));

        assertThat(reconciled.hasUsage()).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
            "peak_native_memory_bytes,native_memory_bytes", "peak_jvm_heap_bytes,jvm_heap_bytes",
            "spill_bytes,spill_bytes", "candidate_rows,candidate_rows", "candidate_bytes,candidate_bytes",
            "shuffle_send_bytes,shuffle_send_bytes", "shuffle_receive_bytes,shuffle_receive_bytes",
            "object_store_read_bytes,object_store_read_bytes", "object_store_write_bytes,object_store_write_bytes",
            "provider_items,provider_items", "provider_attempts,provider_attempts",
            "provider_request_bytes,provider_request_bytes", "provider_response_bytes,provider_response_bytes",
            "media_bytes,media_bytes"})
    void onlyPreservesUsageWithinEveryExactGrant(String usageField, String grantField) throws Exception {
        var grant = StageResourceGrant.newBuilder();
        grant.setField(grant.getDescriptorForType().findFieldByName(grantField), 10L);
        var request = request().setResourceGrant(grant).build();
        var usage = ResourceUsage.newBuilder();
        var field = usage.getDescriptorForType().findFieldByName(usageField);
        usage.setField(field, 10L);
        if (usageField.equals("provider_response_bytes")) {
            usage.setObservedProviderResponseBytes(10);
        }
        var observed = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FAILED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).setUsage(usage);
        var error = new ErrorDetail("TASK_FAILED", "Failure", "source read failed");
        var boundary = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FAILED, error, observed.build().toByteArray()));
        assertThat(boundary.getUsage()).isEqualTo(usage.build());
        for (long value : new long[] {11L, -1L}) {
            usage.setField(field, value);
            observed.setUsage(usage);
            var rejected = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                    request, TaskState.TASK_STATE_FAILED, error, observed.build().toByteArray()));
            assertThat(rejected.hasUsage()).isFalse();
            assertThat(rejected.getCoverage().getCompletedWorkUnits()).isZero();
        }
    }

    @Test
    void preservesOnlyExactNativeFailureAndFallsBackForUntrustworthyEvidence() throws Exception {
        var request = request().build();
        var nativeError = ExecutionError.newBuilder().setCode("NATIVE_SOURCE_FAILURE")
                .setMessage("native failure").build();
        var observed = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FAILED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).setError(nativeError).build();
        var error = new ErrorDetail("WORKER_FAILURE", "Failure", "worker failure");
        var exact = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FAILED, error, observed.toByteArray()));
        assertThat(exact.getError()).isEqualTo(nativeError);
        byte[][] untrustworthy = {
                null, new byte[0], new byte[] {(byte) 0x80},
                observed.toBuilder().setAttempt(request.getAttempt().toBuilder().setAttemptId(1))
                        .build().toByteArray(),
                observed.toBuilder().setState(TaskState.TASK_STATE_CANCELLED).build().toByteArray(),
                observed.toBuilder().setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                        .build().toByteArray(),
                observed.toBuilder().clearError().build().toByteArray(),
                observed.toBuilder().setError(nativeError.toBuilder().clearCode()).build().toByteArray()};
        for (byte[] encoded : untrustworthy) {
            var fallback = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                    request, TaskState.TASK_STATE_FAILED, error, encoded));
            assertThat(fallback.getError().getCode()).isEqualTo("WORKER_FAILURE");
            assertThat(fallback.getCoverage().getCompletedWorkUnits()).isZero();
        }
    }

    @Test
    void rejectsUnrepresentableCpuUsageAndNeverRetainsUsageWithoutATrustworthyGrant() throws Exception {
        var request = request().setResourceGrant(StageResourceGrant.getDefaultInstance()).build();
        var observed = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setUsage(ResourceUsage.newBuilder().setCpuNanos(Long.MAX_VALUE)).build();
        var error = new ErrorDetail("TASK_FAILED", "Failure", "source read failed");
        var valid = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FAILED, error, observed.toByteArray()));
        assertThat(valid.getUsage().getCpuNanos()).isEqualTo(Long.MAX_VALUE);
        var invalidCpu = observed.toBuilder().setUsage(observed.getUsage().toBuilder().setCpuNanos(-1)).build();
        var rejectedCpu = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FAILED, error, invalidCpu.toByteArray()));
        assertThat(rejectedCpu.hasUsage()).isFalse();
        var missingGrant = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.toBuilder().clearResourceGrant().build(), TaskState.TASK_STATE_FAILED,
                error, observed.toByteArray()));
        assertThat(missingGrant.hasUsage()).isFalse();
        var invalidGrant = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request.toBuilder().setResourceGrant(StageResourceGrant.newBuilder().setNativeMemoryBytes(-1)).build(),
                TaskState.TASK_STATE_FAILED, error, observed.toByteArray()));
        assertThat(invalidGrant.hasUsage()).isFalse();
    }

    private static org.pangrova.protocol.v2.TaskRequest.Builder request() {
        return CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", TaskKind.TASK_KIND_READ, new byte[] {1});
    }

    private static DataSplit split(int index, String sourceId) {
        return DataSplit.newBuilder()
                .setSplitId("split-" + index)
                .setSourceId(sourceId)
                .setSourceSnapshot("snapshot-1")
                .build();
    }
}
