package org.pangrova.shuffle.transport;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.shuffle.checksum.ShuffleChecksums;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchStreamAssemblerTest {

    private static final int MAX_BATCH_BYTES = 256 * 1024 * 1024;

    @Test
    void reassemblesCanonicalRowsAndRejectsRowCountDrift() {
        byte[] payload = "canonical-batch".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BatchStreamAssembler assembler = new BatchStreamAssembler(8, 64);

        assertThat(assembler.accept(request(payload, 0, 0, 2, 11))).isNull();
        assertThatThrownBy(() -> assembler.accept(request(payload, 1, 1, 2, 12)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disagree");
    }

    @Test
    void completesOnlyAfterAllChecksummedChunksArrive() {
        byte[] payload = "canonical-batch".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BatchStreamAssembler assembler = new BatchStreamAssembler(8, 64);

        assertThat(assembler.accept(request(payload, 0, 0, 2, 11))).isNull();
        CompleteWriteBatch complete = assembler.accept(request(payload, 1, 1, 2, 11));

        assertThat(complete.canonicalBytes()).isEqualTo(payload);
        assertThat(complete.rowCount()).isEqualTo(11);
        assembler.requireComplete();
    }

    @Test
    void declaredMaximumDoesNotPreallocatePerStream() {
        List<BatchStreamAssembler> streams = new ArrayList<>();

        for (int index = 0; index < 64; index++) {
            BatchStreamAssembler assembler = new BatchStreamAssembler(1024 * 1024,
                    MAX_BATCH_BYTES);
            streams.add(assembler);
            assertThat(assembler.accept(maximumDeclaredFirstChunk())).isNull();
        }

        assertThat(streams).hasSize(64);
    }

    private static WriteBatchRequest request(
            byte[] payload,
            long streamSequence,
            int chunkIndex,
            int chunkCount,
            long rows) {
        int split = Math.min(8, payload.length);
        int offset = chunkIndex == 0 ? 0 : split;
        int length = chunkIndex == 0 ? split : payload.length - split;
        ByteString data = ByteString.copyFrom(payload, offset, length);
        return WriteBatchRequest.newBuilder().setShuffleId("shuffle-1").setPartitionId(0)
                .setStreamSequence(streamSequence).setUncompressedSize(payload.length)
                .setCodec(CompressionCodec.COMPRESSION_CODEC_NONE).setRowCount(rows)
                .setProducerAttempt(attempt()).setAccessCapability(
                        ShuffleAccessCapability.newBuilder().setToken(ByteString.copyFromUtf8("x"))
                                .setExpiresAtEpochMs(Long.MAX_VALUE))
                .setChunk(BatchChunk.newBuilder().setBatchSequence(0)
                        .setChunkIndex(chunkIndex).setChunkCount(chunkCount)
                        .setBatchSizeBytes(payload.length).setData(data)
                        .setChunkChecksum(ShuffleChecksums.protocol(
                                ShuffleChecksums.sha256(data.toByteArray())))
                        .setBatchChecksum(ShuffleChecksums.protocol(
                                ShuffleChecksums.sha256(payload))))
                .build();
    }

    private static WriteBatchRequest maximumDeclaredFirstChunk() {
        ByteString data = ByteString.copyFrom(new byte[] {1});
        return WriteBatchRequest.newBuilder().setShuffleId("shuffle-1").setPartitionId(0)
                .setStreamSequence(0).setUncompressedSize(MAX_BATCH_BYTES)
                .setCodec(CompressionCodec.COMPRESSION_CODEC_NONE).setRowCount(1)
                .setProducerAttempt(attempt()).setAccessCapability(
                        ShuffleAccessCapability.newBuilder().setToken(ByteString.copyFromUtf8("x"))
                                .setExpiresAtEpochMs(Long.MAX_VALUE))
                .setChunk(BatchChunk.newBuilder().setBatchSequence(0).setChunkIndex(0)
                        .setChunkCount(2).setBatchSizeBytes(MAX_BATCH_BYTES).setData(data)
                        .setChunkChecksum(ShuffleChecksums.protocol(
                                ShuffleChecksums.sha256(data.toByteArray())))
                        .setBatchChecksum(ShuffleChecksums.protocol(new byte[32])))
                .build();
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query-1").setStageId(1)
                .setPartitionId(0).setPlanEpoch(2).setAttemptId(0).setFencingToken(3)
                .setTaskId("task-1").setControlEpoch(4)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner")
                        .setOwnerIncarnation("incarnation").setFencingToken(5)).build();
    }
}
