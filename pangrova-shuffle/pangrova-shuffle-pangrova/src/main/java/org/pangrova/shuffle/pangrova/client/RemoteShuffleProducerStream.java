package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.enums.ProducerStreamState;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.PartitionWriteAck;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.protocol.v2.shuffle.WriteBatchResponse;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;
import org.pangrova.protocol.shuffle.codec.CanonicalBatchChunks;

import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

/** One flow-controlled multiplexed stream for a producer and data endpoint. */
final class RemoteShuffleProducerStream implements AutoCloseable {

    private final Object readinessMonitor = new Object();
    private final CountDownLatch completed = new CountDownLatch(1);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicReference<WriteBatchResponse> response = new AtomicReference<>();
    private final AtomicReference<ProducerStreamState> state = new AtomicReference<>(ProducerStreamState.ACTIVE);
    private final java.util.concurrent.atomic.AtomicBoolean abortCompleted =
            new java.util.concurrent.atomic.AtomicBoolean();
    private final Map<Integer, PartitionWriteStats> written = new HashMap<>();
    private final String shuffleId;
    private final ExchangeTaskAttempt taskAttempt;
    private final CompressionCodec codec;
    private final RenewableShuffleAccess access;
    private final long deadlineEpochMillis;
    private final ClientCallStreamObserver<WriteBatchRequest> requestStream;
    private long totalBatches;
    private long totalBytes;
    private long streamSequence;

    RemoteShuffleProducerStream(
            ShuffleServiceGrpc.ShuffleServiceStub stub,
            String shuffleId,
            ExchangeTaskAttempt taskAttempt,
            CompressionCodec codec,
            RenewableShuffleAccess access,
            long deadlineEpochMillis) {
        this.shuffleId = shuffleId;
        this.taskAttempt = taskAttempt;
        this.codec = codec;
        this.access = access;
        this.deadlineEpochMillis = deadlineEpochMillis;
        AtomicReference<ClientCallStreamObserver<WriteBatchRequest>> stream =
                new AtomicReference<>();
        stub.writeBatch(responseObserver(stream));
        requestStream = stream.get();
        if (requestStream == null) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle write stream was not initialized");
        }
    }

    synchronized void write(
            int partitionId,
            long batchSequence,
            long rowCount,
            ByteBuffer data) {
        requireActive();
        PartitionWriteStats stats = written.computeIfAbsent(
                partitionId, ignored -> new PartitionWriteStats());
        if (batchSequence != stats.lastSequence + 1) {
            throw new IllegalArgumentException(
                    "Remote shuffle partition sequence is not contiguous");
        }
        if (rowCount <= 0) {
            throw new IllegalArgumentException("rowCount must be positive");
        }
        ShuffleAccessCapability capability = access.currentAtBoundary(deadlineEpochMillis);
        writeChunks(partitionId, batchSequence, rowCount, data, capability);
        requireActive();
        int byteCount = data.remaining();
        stats.record(batchSequence, byteCount);
        totalBatches++;
        totalBytes += byteCount;
    }

    private void writeChunks(
            int partitionId,
            long batchSequence,
            long rowCount,
            ByteBuffer data,
            ShuffleAccessCapability capability) {
        int chunkCount = CanonicalBatchChunks.chunkCount(data);
        var batchChecksum = CanonicalBatchChunks.sha256(data);
        for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
            requireActive();
            awaitReady();
            requireActive();
            var chunk = CanonicalBatchChunks.chunk(
                    data, batchSequence, chunkIndex, chunkCount, batchChecksum);
            requestStream.onNext(PangrovaShuffleClient.buildWriteRequest(
                    shuffleId, taskAttempt, new PangrovaShuffleClient.WriteFrame(
                            partitionId, streamSequence++, chunk,
                            data.remaining(), rowCount), codec, capability));
        }
    }

    synchronized void finish() {
        if (!state.compareAndSet(ProducerStreamState.ACTIVE, ProducerStreamState.FINISHING)) {
            throw new IllegalStateException("Remote shuffle writer is terminal");
        }
        requestStream.onCompleted();
        awaitCompletion();
        validateResponse();
        if (!state.compareAndSet(ProducerStreamState.FINISHING, ProducerStreamState.FINISHED)) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle writer was aborted during completion");
        }
    }

    void abort(String reason) {
        ProducerStreamState previous = state.getAndSet(ProducerStreamState.ABORTED);
        try {
            if (previous != ProducerStreamState.FINISHED && !abortCompleted.get()) {
                requestStream.cancel(reason, Status.CANCELLED
                        .withDescription(reason).asRuntimeException());
                abortCompleted.set(true);
            }
        } finally {
            signalReady();
        }
    }

    /** Waits for the actual gRPC terminal callback; sending cancel alone is not drain evidence. */
    void awaitTerminal(long cleanupDeadlineNanos) {
        try {
            long remaining = cleanupDeadlineNanos - System.nanoTime();
            if (completed.getCount() != 0 && (remaining <= 0
                    || !completed.await(remaining, TimeUnit.NANOSECONDS))) {
                throw new RemoteShuffleClientException("Remote shuffle producer cancellation has not drained");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException("Remote shuffle cancellation drain interrupted", failure);
        }
    }

    @Override
    public void close() {
        abort("Remote shuffle writer closed before commit");
    }

    private ClientResponseObserver<WriteBatchRequest, WriteBatchResponse>
            responseObserver(
                    AtomicReference<ClientCallStreamObserver<WriteBatchRequest>> stream) {
        return new WriteResponseObserver(stream);
    }

    private final class WriteResponseObserver
            implements ClientResponseObserver<WriteBatchRequest, WriteBatchResponse> {
        private final AtomicReference<ClientCallStreamObserver<WriteBatchRequest>> stream;

        private WriteResponseObserver(AtomicReference<ClientCallStreamObserver<WriteBatchRequest>> stream) {
            this.stream = stream;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<WriteBatchRequest> request) {
            stream.set(request);
            request.setOnReadyHandler(RemoteShuffleProducerStream.this::signalReady);
        }

        @Override
        public void onNext(WriteBatchResponse value) {
            if (!response.compareAndSet(null, value)) {
                error.compareAndSet(null, new RemoteShuffleClientException(
                        "WriteBatch returned more than one response"));
            }
        }

        @Override
        public void onError(Throwable failure) {
            error.compareAndSet(null, failure);
            completed.countDown();
            signalReady();
        }

        @Override
        public void onCompleted() {
            if (state.get() == ProducerStreamState.ACTIVE) {
                error.compareAndSet(null, new RemoteShuffleClientException(
                        "Remote shuffle server completed before the producer finished writing"));
            }
            completed.countDown();
            signalReady();
        }
    }

    private void awaitReady() {
        synchronized (readinessMonitor) {
            while (state.get() == ProducerStreamState.ACTIVE && error.get() == null && !requestStream.isReady()) {
                waitUntilReady();
            }
        }
        requireActive();
        throwIfFailed();
    }

    private void waitUntilReady() {
        long remaining = remainingMillis();
        if (remaining <= 0) {
            abort("Remote shuffle write deadline exceeded");
            throw new RemoteShuffleClientException(
                    "Remote shuffle write deadline exceeded");
        }
        try {
            readinessMonitor.wait(remaining);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            abort("Remote shuffle write interrupted");
            throw new RemoteShuffleClientException(
                    "Remote shuffle write interrupted", failure);
        }
    }

    private void awaitCompletion() {
        try {
            long remaining = remainingMillis();
            if (remaining <= 0 || !completed.await(remaining, TimeUnit.MILLISECONDS)) {
                throw new RemoteShuffleClientException(
                        "Remote shuffle write completion deadline exceeded");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException(
                    "Remote shuffle write completion interrupted", failure);
        }
        throwIfFailed();
    }

    private void validateResponse() {
        WriteBatchResponse accepted = response.get();
        if (accepted == null
                || accepted.getTotalBatchesAccepted() != totalBatches
                || accepted.getTotalBytesAccepted() != totalBytes
                || accepted.getPartitionsCount() != written.size()) {
            throw acknowledgementMismatch();
        }
        Map<Integer, PartitionWriteAck> acknowledgements = new HashMap<>();
        for (PartitionWriteAck acknowledgement : accepted.getPartitionsList()) {
            if (acknowledgements.put(
                    acknowledgement.getPartitionId(), acknowledgement) != null) {
                throw acknowledgementMismatch();
            }
        }
        written.forEach((partitionId, stats) ->
                validatePartitionAck(acknowledgements.get(partitionId), stats));
    }

    private static void validatePartitionAck(
            PartitionWriteAck acknowledgement, PartitionWriteStats stats) {
        if (acknowledgement == null
                || acknowledgement.getBatchesAccepted() != stats.batches
                || acknowledgement.getBytesAccepted() != stats.bytes
                || acknowledgement.getLastPartitionSequence() != stats.lastSequence) {
            throw acknowledgementMismatch();
        }
    }

    private static RemoteShuffleClientException acknowledgementMismatch() {
        return new RemoteShuffleClientException(
                "Remote shuffle write acknowledgement does not match sent data");
    }

    private void throwIfFailed() {
        Throwable failure = error.get();
        if (failure != null) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle write stream failed", failure);
        }
    }

    private void requireActive() {
        if (state.get() != ProducerStreamState.ACTIVE) {
            throw new RemoteShuffleClientException(
                    "Remote shuffle writer is terminal");
        }
    }

    private long remainingMillis() {
        return deadlineEpochMillis - System.currentTimeMillis();
    }

    private void signalReady() {
        synchronized (readinessMonitor) {
            readinessMonitor.notifyAll();
        }
    }

    private static final class PartitionWriteStats {

        private long batches;
        private long bytes;
        private long lastSequence = -1;

        private void record(long sequence, int byteCount) {
            batches++;
            bytes += byteCount;
            lastSequence = sequence;
        }
    }

}
