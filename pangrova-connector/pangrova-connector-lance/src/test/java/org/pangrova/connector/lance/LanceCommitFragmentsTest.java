package org.pangrova.connector.lance;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.write.NativeWriteBinder;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.LanceTransactionReference;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceCommitFragmentsTest {
    private final LanceOutputHandle output = LanceOutputHandleCodecTest.output(
            LanceOutputHandleCodecTest.table("tenant-1", "fragments"));

    @Test
    void acceptsOnlyBoundedExactAttemptReferencesAndSumsWinnerRows() {
        var first = fragment(0);
        var second = fragment(1);
        assertThat(LanceCommitFragments.validate(output,
                List.of(first.toByteArray(), second.toByteArray()), context(binding()))).isEqualTo(4);
        assertRejected(first.toBuilder().setContractVersion(1).build());
        assertRejected(first.toBuilder().setDatasetUri("s3://other/table.lance").build());
        assertRejected(first.toBuilder().setReadVersion(10).build());
        assertRejected(first.toBuilder().setWriteId("other-write").build());
        assertRejected(first.toBuilder().setSchemaFenceSha256(ByteString.EMPTY).build());
    }

    @Test
    void rejectsReferenceTraversalIdentityMismatchAndOversizedTransaction() {
        var fragment = fragment(0);
        assertRejected(fragment.toBuilder().setTransaction(fragment.getTransaction().toBuilder()
                .setRelativeObjectKey("../../transaction.pb")).build());
        assertRejected(fragment.toBuilder().setTransaction(fragment.getTransaction().toBuilder()
                .setTenantId("other-tenant")).build());
        assertRejected(fragment.toBuilder().setTransaction(fragment.getTransaction().toBuilder()
                .setGeneration("other-generation")).build());
        assertRejected(fragment.toBuilder().setTransaction(fragment.getTransaction().toBuilder()
                .setSizeBytes(64 * 1024 * 1024 + 1)).build());
        assertRejected(fragment.toBuilder().setRowsWritten(-1).build());
    }

    @Test
    void rejectsDuplicateWorkUnitsMalformedMetadataAndChangedDurableHandle() {
        byte[] bytes = fragment(0).toByteArray();
        assertThatThrownBy(() -> LanceCommitFragments.validate(output, List.of(bytes, bytes), context(binding())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repeat");
        assertThatThrownBy(() -> LanceCommitFragments.validate(output, List.of(new byte[] {(byte) 0xff}),
                context(binding()))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Malformed");
        var changed = binding().toBuilder().setLance(binding().getLance().toBuilder()
                .setOutputHandle(binding().getLance().getOutputHandle().toBuilder().setPayload(ByteString.EMPTY))).build();
        assertThatThrownBy(() -> LanceCommitFragments.validate(output, List.of(bytes), context(changed)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("durable intent");
        assertThatThrownBy(() -> LanceCommitFragments.validate(output, List.of(), context(binding())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bounded nonempty");
    }

    private void assertRejected(LanceWriteFragment fragment) {
        assertThatThrownBy(() -> LanceCommitFragments.validate(output,
                List.of(fragment.toByteArray()), context(binding()))).isInstanceOf(IllegalArgumentException.class);
    }

    private WriteOutputBinding binding() {
        var codec = new LanceOutputHandleCodec();
        byte[] bytes = codec.serialize(output);
        return WriteOutputBinding.newBuilder().setLance(LanceWriteBinding.newBuilder()
                .setCatalog(org.pangrova.protocol.v2.CatalogExecutionBinding.newBuilder()
                        .setCatalogName(output.table().catalog()))
                .setOutputHandle(SerializedConnectorOutputHandle.newBuilder().setTypeUrl(codec.typeUrl())
                        .setCodecVersion(codec.codecVersion()).setPayload(ByteString.copyFrom(bytes))
                        .setPayloadChecksum(ProtocolChecksums.sha256(bytes)))).build();
    }

    private DistributedWriteCommitContext context(WriteOutputBinding binding) {
        return new DistributedWriteCommitContext(
                "execution-1",
                "tenant-1",
                0,
                binding.toByteArray(),
                "a".repeat(64),
                "alice",
                LanceWriteReceiptFixtures.receipt(), null);
    }

    private LanceWriteFragment fragment(int partition) {
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId("execution-1").setTaskId("task-" + partition)
                .setStageId(1).setPartitionId(partition).setPlanEpoch(2).setControlEpoch(3).setFencingToken(4)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner")
                        .setOwnerIncarnation("incarnation").setFencingToken(5)).build();
        var digest = ProtocolChecksums.sha256(new byte[] {1, 2, 3});
        String key = NativeWriteBinder.attemptPrefix(output.allocation().stagingPrefix(), attempt)
                + "/transaction-" + HexFormat.of().formatHex(digest.getDigest().toByteArray()) + ".pb";
        return LanceWriteFragment.newBuilder().setContractVersion(2).setDatasetUri(output.allocation().datasetUri())
                .setWriteId(output.allocation().writeId()).setTaskAttempt(attempt).setRowsWritten(2).setInputBytes(8)
                .setOperation(LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE).setReadVersion(0)
                .setSchemaFenceSha256(ByteString.copyFrom(output.allocation().schemaFenceSha256()))
                .setTransaction(LanceTransactionReference.newBuilder().setRelativeObjectKey(key).setSizeBytes(3)
                        .setSha256(digest).setTenantId("tenant-1").setWriteId(output.allocation().writeId())
                        .setGeneration(output.allocation().generation()).setTaskAttempt(attempt)).build();
    }
}
