package org.pangrova.worker.exchange.remote;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.ReadPartitionTerminal;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.ShufflePartitionManifestChecksum;
import org.pangrova.shuffle.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.ShuffleServiceAddress;
import org.pangrova.shuffle.exchange.ExchangeAttemptFence;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;
import org.pangrova.worker.exchange.CanonicalBatchChunks;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises read-stream rollover at a verified complete-batch boundary. */
class RemoteShuffleReadRenewalTest {
    private static final byte[] FIRST = "first".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SECOND = "second".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PRODUCER_SET = ShuffleProducerSetChecksum
            .forContiguousPartitions("query-1", 1, 2, 1);

    @Test
    void renewsAndResumesAtNextVerifiedBatchWithoutRepeatingBytes() throws Exception {
        RecordingReadService service = new RecordingReadService();
        Server server = start(service);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext().build();
        long now = System.currentTimeMillis();
        AtomicLong renewalClock = new AtomicLong(now);
        try (RemoteShuffleClient client = new RemoteShuffleClient(channel, 5_000, 5_000);
                RemoteShuffleExchangeSource source = new RemoteShuffleExchangeSource(
                        binding(server.getPort(), now), client, renewalClock::get)) {
            BatchChunk first = source.nextVerifiedChunk();
            assertThat(first.getData().toByteArray()).isEqualTo(FIRST);
            assertThat(source.lastCompletedSequence()).isZero();

            renewalClock.set(now + 70_000L);
            BatchChunk second = source.nextVerifiedChunk();
            assertThat(second.getData().toByteArray()).isEqualTo(SECOND);
            assertThat(source.lastCompletedSequence()).isEqualTo(1L);
            assertThat(source.nextVerifiedChunk()).isNull();
            assertThat(source.isFinished()).isTrue();
            assertThat(service.startSequences).containsExactly(0L, 1L);
            assertThat(service.renewCalls.get()).isEqualTo(1);
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static Server start(RecordingReadService service) throws IOException {
        return ServerBuilder.forPort(0).addService(service).build().start();
    }

    private static ExternalShuffleTransport binding(int port, long now) {
        ShuffleExecutionOwner owner = new ShuffleExecutionOwner("coordinator", "boot-1", 1);
        ExchangeTaskAttempt attempt = new ExchangeTaskAttempt(
                "query-1", 2, 0, "consumer", 1,
                new ExchangeAttemptFence(2, 1, 1), owner);
        return new ExternalShuffleTransport("shuffle-1",
                List.of(new ShuffleServerEndpoint("127.0.0.1", port, 0, 1, "")),
                new ShuffleServiceAddress("127.0.0.1", port, ""),
                0, 1, 2, owner, attempt, now + 120_000L,
                new ShuffleAccess("read-token".getBytes(StandardCharsets.UTF_8),
                        now + 90_000L), 1, PRODUCER_SET);
    }

    private static ReadBatchResponse batch(long sequence, byte[] bytes) {
        BatchChunk chunk = CanonicalBatchChunks.chunk(
                bytes, sequence, 0, 1, CanonicalBatchChunks.sha256(bytes));
        return ReadBatchResponse.newBuilder().setChunk(chunk).setRowCount(1).build();
    }

    private static ReadBatchResponse terminal() {
        byte[] outputChecksum = ShufflePartitionManifestChecksum.accumulator()
                .add(0, FIRST.length, 1, CanonicalBatchChunks.sha256(FIRST).getDigest()
                        .toByteArray())
                .add(1, SECOND.length, 1, CanonicalBatchChunks.sha256(SECOND).getDigest()
                        .toByteArray())
                .finish();
        return ReadBatchResponse.newBuilder().setTerminal(ReadPartitionTerminal.newBuilder()
                .setCommittedLastSequence(1)
                .setOutputChecksum(digest(outputChecksum))
                .setCommittedProducerCount(1).setTotalBytes(FIRST.length + SECOND.length)
                .setTotalRows(2)
                .setProducerSetChecksum(digest(PRODUCER_SET))
                .setGenerationPlanEpoch(2)).build();
    }

    private static Checksum digest(byte[] bytes) {
        return Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(com.google.protobuf.ByteString.copyFrom(bytes)).build();
    }

    private static final class RecordingReadService
            extends ShuffleServiceGrpc.ShuffleServiceImplBase {
        private final List<Long> startSequences = new CopyOnWriteArrayList<>();
        private final AtomicInteger renewCalls = new AtomicInteger();

        @Override
        public void readPartition(
                ReadPartitionRequest request,
                StreamObserver<ReadBatchResponse> response) {
            startSequences.add(request.getStartSequence());
            if (request.getStartSequence() == 0) {
                assertThat(request.getAccessCapability().getToken().toStringUtf8())
                        .isEqualTo("read-token");
                response.onNext(batch(0, FIRST));
                return;
            }
            assertThat(request.getStartSequence()).isEqualTo(1L);
            assertThat(request.getAccessCapability().getToken().toStringUtf8())
                    .isEqualTo("renewed-read-token");
            response.onNext(batch(1, SECOND));
            response.onNext(terminal());
            response.onCompleted();
        }

        @Override
        public void renewAccess(
                RenewAccessRequest request,
                StreamObserver<RenewAccessResponse> response) {
            renewCalls.incrementAndGet();
            assertThat(request.getOperation())
                    .isEqualTo(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ);
            assertThat(request.getTaskAttempt().getTaskId()).isEqualTo("consumer");
            assertThat(request.getPreviousCapability().getToken().toStringUtf8())
                    .isEqualTo("read-token");
            response.onNext(RenewAccessResponse.newBuilder()
                    .setAccessCapability(ShuffleAccessCapability.newBuilder()
                            .setToken(com.google.protobuf.ByteString.copyFromUtf8(
                                    "renewed-read-token"))
                            .setExpiresAtEpochMs(System.currentTimeMillis() + 180_000L))
                    .build());
            response.onCompleted();
        }
    }
}
