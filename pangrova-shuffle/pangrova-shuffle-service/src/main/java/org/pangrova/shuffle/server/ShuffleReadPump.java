package org.pangrova.shuffle.server;

import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;

import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionTerminal;
import org.pangrova.shuffle.config.ShuffleServiceConfig;

import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** Backpressure-aware reader for one finalized output partition. */
final class ShuffleReadPump implements Runnable {
    private static final int MAX_FRAMES_PER_PUMP = 4;
    private static final int BUFFER_PEAK_MULTIPLIER = 3;

    private final ShuffleServiceEngine engine;
    private final ShuffleServiceEngine.ReadResult result;
    private final ServerCallStreamObserver<ReadBatchResponse> response;
    private final Executor executor;
    private final Semaphore memoryBytes;
    private final ShuffleServiceConfig.Limits limits;
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private List<StoredBatch> page = List.of();
    private int pageIndex;
    private long sequence;
    private byte[] payload;
    private byte[] checksum;
    private long rowCount;
    private int chunkIndex;
    private int reservedBytes;
    private boolean running;

    ShuffleReadPump(
            ShuffleServiceEngine engine,
            ShuffleServiceEngine.ReadResult result,
            ServerCallStreamObserver<ReadBatchResponse> response,
            Executor executor,
            Semaphore memoryBytes,
            ShuffleServiceConfig.Limits limits) {
        this.engine = engine;
        this.result = result;
        this.response = response;
        this.executor = executor;
        this.memoryBytes = memoryBytes;
        this.limits = limits;
        this.sequence = result.startSequence();
        if (sequence > finalPartition().committedLastSequence() + 1) {
            throw new IllegalArgumentException("read start sequence exceeds partition end");
        }
    }

    void schedule() {
        if (closed.get() || !response.isReady() || !scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(this);
        } catch (RejectedExecutionException rejected) {
            scheduled.set(false);
            fail(CanonicalShuffleGrpcServer.overloaded());
        }
    }

    @Override
    public void run() {
        setRunning(true);
        try {
            pumpReadyFrames();
        } catch (RuntimeException failure) {
            fail(failure);
        } finally {
            setRunning(false);
            scheduled.set(false);
            schedule();
        }
    }

    void cancel() {
        if (closed.compareAndSet(false, true)) {
            releaseIfIdle();
        }
    }

    private void pumpReadyFrames() {
        int emitted = 0;
        while (!closed.get() && response.isReady() && emitted < MAX_FRAMES_PER_PUMP) {
            if (!preparePayload()) {
                response.onNext(terminal());
                response.onCompleted();
                closed.set(true);
                return;
            }
            if (closed.get()) {
                return;
            }
            response.onNext(batchFrame(sequence, payload, checksum, rowCount, chunkIndex++));
            emitted++;
            if (chunkIndex == chunkCount(payload.length)) {
                releaseLease();
                payload = null;
                checksum = null;
                rowCount = 0;
                sequence++;
            }
        }
    }

    private boolean preparePayload() {
        if (payload != null) {
            return true;
        }
        if (pageIndex == page.size()) {
            page = engine.readPage(result, sequence);
            pageIndex = 0;
            if (page.isEmpty()) {
                return false;
            }
        }
        StoredBatch stored = page.get(pageIndex++);
        reserve(stored.sizeBytes());
        payload = engine.readPayload(result, stored);
        checksum = stored.checksum();
        rowCount = stored.rowCount();
        chunkIndex = 0;
        return true;
    }

    private void reserve(long bytes) {
        int permits = Math.multiplyExact(Math.toIntExact(bytes), BUFFER_PEAK_MULTIPLIER);
        if (!memoryBytes.tryAcquire(permits)) {
            throw Status.RESOURCE_EXHAUSTED.withDescription(
                    "shuffle read memory quota exhausted").asRuntimeException();
        }
        synchronized (this) {
            reservedBytes = permits;
        }
    }

    private ReadBatchResponse batchFrame(
            long batchSequence,
            byte[] canonicalBytes,
            byte[] batchChecksum,
            long batchRowCount,
            int index) {
        int chunks = chunkCount(canonicalBytes.length);
        int offset = index * limits.maxChunkBytes();
        int length = Math.min(limits.maxChunkBytes(), canonicalBytes.length - offset);
        ByteString data = ByteString.copyFrom(canonicalBytes, offset, length);
        BatchChunk chunk = BatchChunk.newBuilder()
                .setBatchSequence(batchSequence).setChunkIndex(index).setChunkCount(chunks)
                .setBatchSizeBytes(canonicalBytes.length).setData(data)
                .setChunkChecksum(ShuffleChecksums.protocol(
                        ShuffleChecksums.sha256(data.toByteArray())))
                .setBatchChecksum(ShuffleChecksums.protocol(batchChecksum)).build();
        return ReadBatchResponse.newBuilder().setChunk(chunk)
                .setRowCount(batchRowCount).build();
    }

    private int chunkCount(int payloadBytes) {
        return Math.toIntExact((payloadBytes + (long) limits.maxChunkBytes() - 1)
                / limits.maxChunkBytes());
    }

    private ReadBatchResponse terminal() {
        FinalizedPartition partition = finalPartition();
        return ReadBatchResponse.newBuilder().setTerminal(
                ReadPartitionTerminal.newBuilder()
                        .setCommittedLastSequence(partition.committedLastSequence())
                        .setOutputChecksum(ShuffleChecksums.protocol(partition.checksum()))
                        .setCommittedProducerCount(result.finalization().committedProducerCount())
                        .setTotalBytes(partition.totalBytes()).setTotalRows(partition.totalRows())
                        .setProducerSetChecksum(ShuffleChecksums.protocol(
                                result.finalization().producerSetChecksum()))
                        .setGenerationPlanEpoch(
                                result.allocation().identity().planEpoch())).build();
    }

    private FinalizedPartition finalPartition() {
        return result.finalization().partitions().stream()
                .filter(candidate -> candidate.partitionId() == result.partitionId())
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "finalized shuffle lacks requested partition"));
    }

    private void fail(RuntimeException failure) {
        if (closed.compareAndSet(false, true)) {
            releaseIfIdle();
            response.onError(CanonicalShuffleGrpcServer.status(failure));
        }
    }

    private synchronized void setRunning(boolean value) {
        running = value;
        if (!value && closed.get()) {
            releaseLease();
        }
    }

    private synchronized void releaseIfIdle() {
        if (!running) {
            releaseLease();
        }
    }

    private synchronized void releaseLease() {
        if (reservedBytes > 0) {
            memoryBytes.release(reservedBytes);
            reservedBytes = 0;
        }
    }
}
