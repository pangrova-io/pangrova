package org.pangrova.worker.exchange.remote;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.protocol.v1.shuffle.CleanupRequest;
import org.pangrova.protocol.v1.shuffle.CleanupResponse;
import org.pangrova.protocol.v1.shuffle.CommitPartitionRequest;
import org.pangrova.protocol.v1.shuffle.CommitPartitionResponse;
import org.pangrova.protocol.v1.shuffle.CompressionCodec;
import org.pangrova.protocol.v1.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v1.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v1.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v1.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v1.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v1.shuffle.WriteBatchRequest;
import org.pangrova.protocol.v1.shuffle.WriteBatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/**
 * gRPC client wrapper for communicating with a remote shuffle server.
 * Provides typed methods for write, commit, and read operations.
 *
 * <p>Thread-safe: gRPC stubs are thread-safe. Each RemoteShuffleClient wraps
 * a single ManagedChannel to one remote shuffle server endpoint.</p>
 */
public final class RemoteShuffleClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleClient.class);

    private final ManagedChannel channel;
    private final ShuffleServiceGrpc.ShuffleServiceBlockingStub blockingStub;
    private final ShuffleServiceGrpc.ShuffleServiceStub asyncStub;
    private final long writeTimeoutMs;
    private final long readTimeoutMs;

    /** Creates a shuffle client over the given channel with write/read timeouts. */
    public RemoteShuffleClient(ManagedChannel channel, long writeTimeoutMs, long readTimeoutMs) {
        this.channel = channel;
        this.blockingStub = ShuffleServiceGrpc.newBlockingStub(channel);
        this.asyncStub = ShuffleServiceGrpc.newStub(channel);
        this.writeTimeoutMs = writeTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    /**
     * Registers a shuffle on the remote shuffle server.
     */
    public RegisterShuffleResponse registerShuffle(RegisterShuffleRequest request) {
        return blockingStub.withDeadlineAfter(writeTimeoutMs, TimeUnit.MILLISECONDS)
                .registerShuffle(request);
    }

    /**
     * Writes a single batch to the remote shuffle server.
     */
    public void writeBatch(String shuffleId, int partitionId, long sequence,
                           byte[] batchData, int uncompressedSize, CompressionCodec codec) {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        StreamObserver<WriteBatchRequest> writer = asyncStub.writeBatch(
                completionObserver(done, error));

        writer.onNext(buildWriteRequest(shuffleId, partitionId, sequence,
                batchData, uncompressedSize, codec));
        writer.onCompleted();

        awaitWriteCompletion(done, error);
    }

    private static StreamObserver<WriteBatchResponse> completionObserver(
            CountDownLatch done, AtomicReference<Throwable> error) {
        return new StreamObserver<>() {
            @Override public void onNext(WriteBatchResponse value) {}
            @Override
            public void onError(Throwable t) {
                error.set(t);
                done.countDown();
            }
            @Override public void onCompleted() { done.countDown(); }
        };
    }

    private static WriteBatchRequest buildWriteRequest(String shuffleId, int partitionId,
            long sequence, byte[] batchData, int uncompressedSize, CompressionCodec codec) {
        return WriteBatchRequest.newBuilder()
                .setShuffleId(shuffleId)
                .setPartitionId(partitionId)
                .setSequence(sequence)
                .setArrowIpcData(ByteString.copyFrom(batchData))
                .setUncompressedSize(uncompressedSize)
                .setCodec(codec)
                .build();
    }

    private void awaitWriteCompletion(CountDownLatch done, AtomicReference<Throwable> error) {
        try {
            if (!done.await(writeTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new RemoteShuffleClientException(
                        "WriteBatch timed out after " + writeTimeoutMs + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException("WriteBatch interrupted", e);
        }
        if (error.get() != null) {
            throw new RemoteShuffleClientException("WriteBatch failed", error.get());
        }
    }

    /**
     * Commits a partition on the remote shuffle server.
     */
    public CommitPartitionResponse commitPartition(String shuffleId, int partitionId) {
        return blockingStub.withDeadlineAfter(writeTimeoutMs, TimeUnit.MILLISECONDS)
                .commitPartition(CommitPartitionRequest.newBuilder()
                        .setShuffleId(shuffleId)
                        .setPartitionId(partitionId)
                        .build());
    }

    /**
     * Reads the next batch from a partition (blocking mode — waits for commit).
     * Returns an iterator over ReadBatchResponse.
     */
    public java.util.Iterator<ReadBatchResponse> readPartition(String shuffleId, int partitionId) {
        return blockingStub.withDeadlineAfter(readTimeoutMs, TimeUnit.MILLISECONDS)
                .readPartition(ReadPartitionRequest.newBuilder()
                        .setShuffleId(shuffleId)
                        .setPartitionId(partitionId)
                        .build());
    }

    /**
     * Reads the next batch from a partition (streaming mode — data before commit).
     */
    public java.util.Iterator<ReadBatchResponse> readPartitionStreaming(String shuffleId, int partitionId) {
        return blockingStub.withDeadlineAfter(readTimeoutMs, TimeUnit.MILLISECONDS)
                .readPartitionStreaming(ReadPartitionRequest.newBuilder()
                        .setShuffleId(shuffleId)
                        .setPartitionId(partitionId)
                        .build());
    }

    /**
     * Cleans up all shuffle data for a query on the remote shuffle server.
     *
     * @param queryId the query whose shuffle data should be cleaned up
     * @return the cleanup response with partition and byte counts
     */
    public CleanupResponse cleanup(String queryId) {
        return blockingStub.withDeadlineAfter(writeTimeoutMs, TimeUnit.MILLISECONDS)
                .cleanup(CleanupRequest.newBuilder()
                        .setQueryId(queryId)
                        .build());
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
