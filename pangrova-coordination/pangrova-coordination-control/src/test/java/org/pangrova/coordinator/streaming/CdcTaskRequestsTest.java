package org.pangrova.coordinator.streaming;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.ConnectorWriteBinding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcTaskRequestsTest {

    @Test
    void createsAValidatedBoundedCdcTask() {
        TaskRequest request = CdcTaskRequests.create(
                attempt(), security(), grant(), 4_102_444_800_000L, binding());

        assertThat(request.getTaskKind()).isEqualTo(TaskKind.TASK_KIND_CDC_INGEST);
        assertThat(request.getSubstraitPlan()).isEmpty();
        assertThat(request.getSplitsCount()).isZero();
        assertThat(request.getCdcIngest().getRanges(0).getStartOffset()).isEqualTo(10);
        assertThat(request.getCdcIngest().getRanges(0).getEndOffset()).isEqualTo(20);
    }

    @Test
    void rejectsARequestCarryingANativePlan() {
        assertThatThrownBy(() -> CdcTaskRequests.create(
                attempt(), security(), grant(), 4_102_444_800_000L,
                binding().toBuilder().setStreamId(" ").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("durable source/write identity");
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder()
                .setQueryOrJobId("query-1").setTaskId("task-1")
                .setPlanEpoch(1).setFencingToken(7).setControlEpoch(1)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator-a")
                        .setOwnerIncarnation("process-a")
                        .setFencingToken(7))
                .build();
    }

    private static SecurityContext security() {
        return SecurityContext.newBuilder()
                .setTenantId("tenant-a").setPrincipal("test-user").build();
    }

    private static StageResourceGrant grant() {
        return StageResourceGrant.newBuilder()
                .setGrantId("task-1:grant").setCpuSlots(1)
                .setNativeMemoryBytes(1_048_576).build();
    }

    private static CdcIngestBinding binding() {
        return CdcIngestBinding.newBuilder()
                .setStreamId("stream-1")
                .setTenantId("tenant-a")
                .setSourceOwnershipEpoch("epoch-1")
                .setSourceProvider("kafka")
                .setFormatId("debezium-json")
                .setTargetTableUuid("00000000-0000-0000-0000-000000000001")
                .setCommitIntentId("commit-1")
                .setBaseSnapshotId(42L)
                .setCommitIdentitySha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(new byte[32])))
                .setWrite(WriteOutputBinding.newBuilder()
                        .setConnector(ConnectorWriteBinding.getDefaultInstance()))
                .addRanges(CdcSourceRange.newBuilder()
                        .setTopic("orders").setPartition(0)
                        .setStartOffset(10).setEndOffset(20))
                .build();
    }
}
