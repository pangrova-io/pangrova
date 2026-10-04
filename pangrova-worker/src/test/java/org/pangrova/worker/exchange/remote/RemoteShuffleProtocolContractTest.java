package org.pangrova.worker.exchange.remote;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.worker.exchange.CanonicalBatchChunks;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.exchange.ExchangeAttemptFence;
import org.pangrova.shuffle.exchange.ExchangeChannelContext;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
import org.junit.jupiter.api.Test;

class RemoteShuffleProtocolContractTest {

    @Test
    void writesCarryCanonicalAttemptAndBatchChecksum() {
        byte[] batch = "arrow-ipc".getBytes(StandardCharsets.UTF_8);

        var chunk = CanonicalBatchChunks.chunk(
                batch, 3L, 0, 1, CanonicalBatchChunks.sha256(batch));
        var request = RemoteShuffleClient.buildWriteRequest(
                "shuffle-1", context().taskAttempt(),
                new RemoteShuffleClient.WriteFrame(5, 7L, chunk, batch.length, 9L),
                CompressionCodec.COMPRESSION_CODEC_NONE, capability());

        assertThat(request.getShuffleId()).isEqualTo("shuffle-1");
        assertThat(request.getPartitionId()).isEqualTo(5);
        assertThat(request.getStreamSequence()).isEqualTo(7L);
        assertThat(request.getChunk().getBatchSequence()).isEqualTo(3L);
        assertThat(request.getUncompressedSize()).isEqualTo(batch.length);
        assertThat(request.getRowCount()).isEqualTo(9L);
        assertThat(request.getProducerAttempt()).isEqualTo(
                RemoteShuffleClient.toProtocol(context().taskAttempt()));
        assertThat(request.getChunk().getBatchChecksum()).isEqualTo(
                CanonicalTaskRequests.sha256(batch));
        assertThat(request.getAccessCapability().getToken().toStringUtf8())
                .isEqualTo("signed-write-token");
    }

    @Test
    void commitsCarryAttemptSequenceAndUnmodifiedOutputDigest() throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest("all-output".getBytes(StandardCharsets.UTF_8));

        var request = RemoteShuffleClient.buildCommitRequest(
                "shuffle-1", context().taskAttempt(), List.of(
                        PartitionCommitSummary.newBuilder()
                                .setPartitionId(5)
                                .setExpectedLastSequence(7L)
                                .setOutputChecksum(Checksum.newBuilder()
                                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                                        .setDigest(com.google.protobuf.ByteString.copyFrom(digest)))
                                .setTotalBytes(42L)
                                .setTotalBatches(8L)
                                .build()), capability());

        assertThat(request.getPartitions(0).getPartitionId()).isEqualTo(5);
        assertThat(request.getPartitions(0).getExpectedLastSequence()).isEqualTo(7L);
        assertThat(request.getProducerAttempt()).isEqualTo(
                RemoteShuffleClient.toProtocol(context().taskAttempt()));
        assertThat(request.getPartitions(0).getOutputChecksum().getAlgorithm())
                .isEqualTo(Checksum.Algorithm.ALGORITHM_SHA256);
        assertThat(request.getPartitions(0).getOutputChecksum().getDigest().toByteArray())
                .isEqualTo(digest);
        assertThat(request.getAccessCapability().getToken().toStringUtf8())
                .isEqualTo("signed-write-token");
    }

    @Test
    void readsCarryConsumerAttemptAndAbsoluteDeadline() {
        ExchangeChannelContext context = context();
        var access = capability();
        var request = RemoteShuffleClient.buildReadRequest(
                "shuffle-1", context, access);

        assertThat(request.getPartitionId()).isEqualTo(5);
        assertThat(request.getConsumerAttempt()).isEqualTo(
                RemoteShuffleClient.toProtocol(context));
        assertThat(request.getDeadlineEpochMs()).isEqualTo(
                context.deadlineEpochMillis());
        assertThat(request.getAccessCapability()).isEqualTo(access);
    }

    private static ExchangeChannelContext context() {
        return new ExchangeChannelContext(
                new ExchangeTaskAttempt(
                        "query-1", 3, 2, "task-3-2", 13L,
                        new ExchangeAttemptFence(11L, 4, 47L),
                        new ShuffleExecutionOwner(
                                "coordinator-1", "incarnation-1", 23L)),
                5, System.currentTimeMillis() + 60_000L);
    }

    private static org.pangrova.protocol.v2.ShuffleAccessCapability capability() {
        return org.pangrova.protocol.v2.ShuffleAccessCapability.newBuilder()
                .setToken(com.google.protobuf.ByteString.copyFromUtf8("signed-write-token"))
                .setExpiresAtEpochMs(System.currentTimeMillis() + 60_000L)
                .build();
    }
}
