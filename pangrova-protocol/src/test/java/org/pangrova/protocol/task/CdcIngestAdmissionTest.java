package org.pangrova.protocol.task;

import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteOutputBinding;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcIngestAdmissionTest {

    @Test
    void acceptsExactBoundedCdcIngest() {
        assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(valid().build()))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsCdcIngestWithoutItsBinding() {
        var request = valid().clearCdcIngest();
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                .hasMessageContaining("bounded source binding");
    }

    @Test
    void rejectsCdcIngestCarryingAPlan() {
        var request = valid().setSubstraitPlan(ByteString.copyFrom(new byte[] {1}))
                .setSubstraitPlanChecksum(ProtocolChecksums.sha256(new byte[] {1}));
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                .hasMessageContaining("cannot carry a plan");
    }

    @Test
    void rejectsUnboundedSourceRange() {
        var request = valid();
        request.setCdcIngest(request.getCdcIngest().toBuilder()
                .addRanges(CdcSourceRange.newBuilder()
                        .setTopic("events").setPartition(0).setStartOffset(5).setEndOffset(5)));
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                .hasMessageContaining("bounded topic partition interval");
    }

    @Test
    void rejectsTenantMismatch() {
        var request = valid();
        request.setCdcIngest(request.getCdcIngest().toBuilder().setTenantId("other"));
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                .hasMessageContaining("tenant differs");
    }

    private static TaskRequest.Builder valid() {
        var builder = CanonicalTaskRequestTestFixture.builder(
                "cdc-task", "cdc-query", TaskKind.TASK_KIND_CDC_INGEST, new byte[0])
                .clearSubstraitPlan()
                .clearSubstraitPlanChecksum();
        return builder.setCdcIngest(CdcIngestBinding.newBuilder()
                .setStreamId("stream-1")
                .setTenantId("tenant")
                .setSourceOwnershipEpoch("epoch-1")
                .setSourceProvider("kafka")
                .setFormatId("debezium-json")
                .addRanges(CdcSourceRange.newBuilder()
                        .setTopic("events").setPartition(0).setStartOffset(10).setEndOffset(20))
                .setWrite(WriteOutputBinding.newBuilder().setConnector(
                        ConnectorWriteBinding.getDefaultInstance()))
                .setCommitIntentId("intent-1")
                .setCommitIdentitySha256(ProtocolChecksums.sha256(new byte[] {7}))
                .setTargetTableUuid("table-uuid")
                .setBaseSnapshotId(3));
    }
}
