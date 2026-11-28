package org.pangrova.shuffle.pangrova.client;

import com.google.protobuf.ByteString;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ShufflePartitionPublication;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.ReadPartitionTerminal;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.identity.ExchangeAttemptFence;
import org.pangrova.shuffle.api.identity.ExchangeChannelContext;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;
import org.pangrova.shuffle.api.worker.ShuffleChannelLimits;
import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteShuffleRangeReadTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void verifiesAbsoluteSequencesAndRejectsATerminalForAnotherRange(boolean wrongStart) throws Exception {
        byte[] payload = {1, 2, 3, 4, 5};
        byte[] batchDigest = DigestUtils.sha256().digest(payload);
        byte[] manifest = ShufflePartitionManifestChecksum.accumulator(4)
                .add(4, payload.length, 2, batchDigest).finishNonEmpty();
        var range = ShufflePartitionPublication.newBuilder().setStartSequence(4).setLastSequence(4)
                .setTotalBytes(payload.length).setTotalRows(2).setManifestSha256(ByteString.copyFrom(manifest)).build();
        var received = new AtomicReference<ReadPartitionRequest>();
        var service = new ShuffleServiceGrpc.ShuffleServiceImplBase() {
            @Override
            public void readPartition(ReadPartitionRequest request, StreamObserver<ReadBatchResponse> out) {
                received.set(request);
                out.onNext(ReadBatchResponse.newBuilder().setRowCount(2).setChunk(BatchChunk.newBuilder()
                        .setBatchSequence(4).setChunkCount(1).setBatchSizeBytes(payload.length)
                        .setData(ByteString.copyFrom(payload)).setChunkChecksum(checksum(batchDigest))
                        .setBatchChecksum(checksum(batchDigest))).build());
                out.onNext(ReadBatchResponse.newBuilder().setTerminal(ReadPartitionTerminal.newBuilder()
                        .setCertifiedStartSequence(wrongStart ? 3 : 4).setCommittedLastSequence(4)
                        .setTotalBytes(payload.length).setTotalRows(2).setGenerationPlanEpoch(2)
                        .setCommittedProducerCount(4).setProducerSetChecksum(checksum(new byte[32]))
                        .setOutputChecksum(checksum(manifest))).build());
                out.onCompleted();
            }
        };
        var server = ServerBuilder.forPort(0).addService(service).build().start();
        var admission = new RemoteShuffleChannelAdmission(new ShuffleChannelLimits(1, 1));
        long deadline = System.currentTimeMillis() + 60_000;
        try (var pool = new RemoteShuffleClientPool(30_000, 30_000,
                RemoteShuffleClientPool.PoolOptions.defaults(), admission)) {
            var lease = pool.borrow(new ShuffleServiceAddress("127.0.0.1", server.getPort(), ""));
            var attempt = new ExchangeTaskAttempt("q", 2, 1, "task", 3,
                    new ExchangeAttemptFence(2, 0, 1), new ShuffleExecutionOwner("owner", "incarnation", 1),
                    "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
            var binding = new PangrovaPartitionRead("shuffle", new ExchangeChannelContext(attempt, 0, deadline),
                    2, 4, new byte[32], range);
            try (var source = new RemoteShuffleExchangeSource(binding, new ShuffleAccess(new byte[] {1}, deadline),
                    lease)) {
                assertThat(source.nextVerifiedChunk().getBatchSequence()).isEqualTo(4);
                if (wrongStart) {
                    assertThatThrownBy(source::nextVerifiedChunk).isInstanceOf(RemoteShuffleClientException.class)
                            .hasMessageContaining("coordinator publication");
                } else {
                    assertThat(source.nextVerifiedChunk()).isNull();
                    assertThat(source.lastCompletedSequence()).isEqualTo(4);
                }
                assertThat(received.get().getStartSequence()).isEqualTo(4);
                assertThat(received.get().getReadRange().getManifestSha256()).isEqualTo(ByteString.copyFrom(manifest));
            }
        } finally {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static Checksum checksum(byte[] digest) {
        return Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(digest)).build();
    }
}
