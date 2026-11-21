package org.pangrova.shuffle.pangrova.client;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptResponse;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.identity.ExchangeAttemptFence;
import org.pangrova.shuffle.api.identity.ExchangeChannelContext;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteShuffleRpcCancellationTest {
    @Test
    void closeCancelsPendingRenewalAndJoinsReaderBeforeReturningChannelLease() throws Exception {
        var service = new BlockingRenewalService();
        var server = ServerBuilder.forPort(0).addService(service).build().start();
        var executor = Executors.newSingleThreadExecutor();
        var admission = new RemoteShuffleChannelAdmission(new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(1, 1));
        try (var pool = new RemoteShuffleClientPool(30_000, 30_000,
                RemoteShuffleClientPool.PoolOptions.defaults(), admission)) {
            var lease = pool.borrow(new ShuffleServiceAddress("127.0.0.1", server.getPort(), ""));
            var context = new ExchangeChannelContext(attempt(), 0, System.currentTimeMillis() + 60_000);
            var binding = new PangrovaPartitionRead("shuffle", context, 2, 4, new byte[32],
                    new org.pangrova.shuffle.api.ShuffleCommitReceipt.Partition(-1, 0, 0, "00".repeat(32)));
            var grant = new ShuffleAccess(new byte[] {1}, System.currentTimeMillis() + 10_000);
            try (var source = new RemoteShuffleExchangeSource(binding, grant, lease)) {
                assertThat(service.reading.await(5, TimeUnit.SECONDS)).isTrue();
                var read = executor.submit(source::nextVerifiedChunk);
                assertThat(service.renewing.await(5, TimeUnit.SECONDS)).isTrue();
                source.close();
                assertThat(service.renewalCancelled.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> read.get(1, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
                assertThatThrownBy(lease::client).isInstanceOf(IllegalStateException.class);
                assertThat(service.reads.get()).isEqualTo(1);
            }
        } finally {
            executor.shutdownNow();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void abortDrainsEmptyProducerWithUnconfirmedCommit() throws Exception {
        var committing = new CountDownLatch(1);
        var cancelled = new CountDownLatch(1);
        var service = new ShuffleServiceGrpc.ShuffleServiceImplBase() {
            @Override
            public void commitProducerAttempt(CommitProducerAttemptRequest request,
                    StreamObserver<CommitProducerAttemptResponse> response) {
                Context.current().addListener(ignored -> cancelled.countDown(), Runnable::run);
                committing.countDown();
            }
        };
        var server = ServerBuilder.forPort(0).addService(service).build().start();
        var executor = Executors.newSingleThreadExecutor();
        var admission = new RemoteShuffleChannelAdmission(new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(1, 1));
        try (var pool = new RemoteShuffleClientPool(30_000, 30_000,
                RemoteShuffleClientPool.PoolOptions.defaults(), admission)) {
            var address = new ShuffleServiceAddress("127.0.0.1", server.getPort(), "");
            long deadline = System.currentTimeMillis() + 60_000;
            var capability = ShuffleAccessCapability.newBuilder().setToken(ByteString.copyFromUtf8("write"))
                    .setExpiresAtEpochMs(deadline).build();
            var binding = new RemoteShuffleTaskEgress.Binding("shuffle",
                    List.of(new ShuffleServerEndpoint(address.host(), address.port(), 0, 1, "")), address, attempt(), deadline,
                    CompressionCodec.COMPRESSION_CODEC_NONE, capability, TaskAdmissionSealRequest.getDefaultInstance());
            var producer = new RemoteShuffleTaskEgress(binding, pool);
            try {
                var finish = executor.submit(producer::finish);
                assertThat(committing.await(5, TimeUnit.SECONDS)).isTrue();
                producer.abort("cancelled");
                assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> finish.get(1, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            } finally {
                producer.abort("closed");
            }
        } finally {
            executor.shutdownNow();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ExchangeTaskAttempt attempt() {
        var owner = new ShuffleExecutionOwner("owner", "incarnation", 1);
        return new ExchangeTaskAttempt("q", 2, 1, "task", 3,
                new ExchangeAttemptFence(2, 0, 1), owner,
                "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
    }

    private static final class BlockingRenewalService extends ShuffleServiceGrpc.ShuffleServiceImplBase {
        final AtomicInteger renewals = new AtomicInteger();
        final AtomicInteger reads = new AtomicInteger();
        final CountDownLatch renewing = new CountDownLatch(1);
        final CountDownLatch renewalCancelled = new CountDownLatch(1);
        final CountDownLatch reading = new CountDownLatch(1);

        @Override
        public void renewAccess(RenewAccessRequest request, StreamObserver<RenewAccessResponse> response) {
            if (renewals.incrementAndGet() == 1) {
                response.onNext(RenewAccessResponse.newBuilder().setAccessCapability(
                        ShuffleAccessCapability.newBuilder().setToken(ByteString.copyFromUtf8("renewed"))
                                .setExpiresAtEpochMs(System.currentTimeMillis() + 25_000)).build());
                response.onCompleted();
                return;
            }
            Context.current().addListener(ignored -> renewalCancelled.countDown(), Runnable::run);
            renewing.countDown();
        }

        @Override
        public void readPartition(ReadPartitionRequest request, StreamObserver<ReadBatchResponse> response) {
            reads.incrementAndGet();
            reading.countDown();
        }
    }
}
