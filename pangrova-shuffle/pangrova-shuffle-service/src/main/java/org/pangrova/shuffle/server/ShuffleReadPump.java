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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.ReadBatch;

/** Backpressure-aware reader for one finalized output partition. */
final class ShuffleReadPump implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadPump.class);
    private static final int MAX_FRAMES_PER_PUMP = 4;
    private static final int BUFFER_PEAK_MULTIPLIER = 3;

    private final ShuffleServiceEngine engine;
    private final ShuffleServiceEngine.ReadResult result;
    private final ServerCallStreamObserver<ReadBatchResponse> response;
    private final Executor executor;
    private final Semaphore memoryBytes;
    private final Semaphore indexMemoryBytes;
    private final int indexReservation;
    private final ShuffleServiceConfig.Limits limits;
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private List<ReadBatch> page = List.of();
    private int pageIndex;
    private long sequence;
    private byte[] payload;
    private byte[] checksum;
    private long rowCount;
    private int chunkIndex;
    private int reservedBytes;
    private boolean running;
    private boolean pinReleased;
    private boolean indexReleased;

    ShuffleReadPump(
            ShuffleServiceEngine engine,
            ShuffleServiceEngine.ReadResult result,
            ServerCallStreamObserver<ReadBatchResponse> response,
            Executor executor,
            Semaphore memoryBytes,
            Semaphore indexMemoryBytes,
            ShuffleServiceConfig.Limits limits) {
        this.engine = engine;
        this.result = result;
        this.response = response;
        this.executor = executor;
        this.memoryBytes = memoryBytes;
        this.indexMemoryBytes = indexMemoryBytes;
        this.indexReservation = result.manifest().memoryReservation(result.startSequence(), result.endSequenceExclusive());
        this.limits = limits;
        this.sequence = result.startSequence();
        if (!indexMemoryBytes.tryAcquire(indexReservation)) {
            var failure = Status.RESOURCE_EXHAUSTED
                    .withDescription("shuffle read index memory quota exhausted").asRuntimeException();
            try {
                result.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
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
            releaseIfIdle(null);
        }
    }

    private void pumpReadyFrames() {
        int emitted = 0;
        while (!closed.get() && response.isReady() && emitted < MAX_FRAMES_PER_PUMP) {
            if (!preparePayload()) {
                var terminal = terminal();
                engine.requireReadAuthority(result);
                if (closed.get()) return;
                response.onNext(terminal);
                // Keep the successor fence through the last frame and drain the pin before RPC completion.
                releaseRead();
                response.onCompleted();
                closed.set(true);
                return;
            }
            if (closed.get()) {
                return;
            }
            var frame = batchFrame(sequence, payload, checksum, rowCount, chunkIndex);
            engine.requireReadAuthority(result);
            if (closed.get()) return;
            response.onNext(frame);
            chunkIndex++;
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
        if (sequence == result.endSequenceExclusive()) {
            if (pageIndex != page.size()) {
                throw new IllegalStateException("finalized shuffle partition exceeds its committed sequence");
            }
            return false;
        }
        if (pageIndex == page.size()) {
            page = engine.readPage(result, sequence);
            pageIndex = 0;
            if (page.isEmpty()) {
                throw new IllegalStateException("finalized shuffle partition is missing committed batches");
            }
        }
        ReadBatch stored = page.get(pageIndex++);
        if (stored.outputSequence() != sequence) {
            throw new IllegalStateException("finalized shuffle output sequence differs from read cursor");
        }
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
                        ShuffleChecksums.sha256(canonicalBytes, offset, length)))
                .setBatchChecksum(ShuffleChecksums.protocol(batchChecksum)).build();
        return ReadBatchResponse.newBuilder().setChunk(chunk)
                .setRowCount(batchRowCount).build();
    }

    private int chunkCount(int payloadBytes) {
        return Math.toIntExact((payloadBytes + (long) limits.maxChunkBytes() - 1)
                / limits.maxChunkBytes());
    }

    private ReadBatchResponse terminal() {
        FinalizedPartition partition = result.partition();
        var terminal = ReadPartitionTerminal.newBuilder()
                .setCommittedLastSequence(result.endSequenceExclusive() - 1)
                .setCommittedProducerCount(result.committedProducerCount())
                .setProducerSetChecksum(ShuffleChecksums.protocol(result.producerSetChecksum()))
                .setGenerationPlanEpoch(result.planEpoch());
        var range = result.range();
        if (range == null) {
            terminal.setTotalBytes(partition.totalBytes()).setTotalRows(partition.totalRows())
                    .setOutputChecksum(ShuffleChecksums.protocol(partition.checksum()));
        } else {
            terminal.setCertifiedStartSequence(range.startSequence())
                    .setTotalBytes(range.bytes()).setTotalRows(range.rows())
                    .setOutputChecksum(ShuffleChecksums.protocol(
                            java.util.HexFormat.of().parseHex(range.manifestSha256())));
        }
        return ReadBatchResponse.newBuilder().setTerminal(terminal).build();
    }

    private void fail(RuntimeException failure) {
        if (closed.compareAndSet(false, true)) {
            releaseIfIdle(failure);
            response.onError(CanonicalShuffleGrpcServer.status(failure));
        }
    }

    private synchronized void setRunning(boolean value) {
        running = value;
        if (!value && closed.get()) {
            releaseAfterClose(null);
        }
    }

    private synchronized void releaseIfIdle(RuntimeException primaryFailure) {
        if (!running) {
            releaseAfterClose(primaryFailure);
        }
    }

    // An uncommitted durable release leaves the existing protection window intact.
    // It must not suppress the terminal stream error or escape a cancellation callback.
    private void releaseAfterClose(RuntimeException primaryFailure) {
        try {
            releaseRead();
        } catch (RuntimeException releaseFailure) {
            if (primaryFailure != null && primaryFailure != releaseFailure) {
                primaryFailure.addSuppressed(releaseFailure);
            }
            LOG.warn("event=shuffle_read_release_failed", releaseFailure);
        }
    }

    private void releaseRead() {
        releaseLease();
        try {
            if (!pinReleased) {
                result.close();
                pinReleased = true;
            }
        } finally {
            if (!indexReleased) {
                page = List.of();
                payload = null;
                checksum = null;
                indexMemoryBytes.release(indexReservation);
                indexReleased = true;
            }
        }
    }

    private synchronized void releaseLease() {
        if (reservedBytes > 0) {
            memoryBytes.release(reservedBytes);
            reservedBytes = 0;
        }
    }
}
