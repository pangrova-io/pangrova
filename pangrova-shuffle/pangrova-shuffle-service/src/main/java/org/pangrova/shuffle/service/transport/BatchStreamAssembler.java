package org.pangrova.shuffle.service.transport;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.shuffle.service.checksum.ShuffleChecksums;
import org.pangrova.shuffle.service.server.error.FailureKind;
import org.pangrova.shuffle.service.server.error.ShuffleServiceFailure;

/** Strict bounded reassembly for one producer-attempt client stream. */
public final class BatchStreamAssembler {

    private final int maxChunkBytes;
    private final int maxBatchBytes;
    private final Map<BatchKey, PartialBatch> partial = new HashMap<>();
    private TaskAttemptId streamAttempt;
    private String streamShuffleId;
    private long nextStreamSequence;
    private long bufferedBytes;

    /** Creates a bounded assembler for one producer RPC stream. */
    public BatchStreamAssembler(int maxChunkBytes, int maxBatchBytes) {
        this.maxChunkBytes = maxChunkBytes;
        this.maxBatchBytes = maxBatchBytes;
    }

    /** Accepts one ordered chunk and returns a batch only when it is complete. */
    public CompleteWriteBatch accept(WriteBatchRequest request) {
        requireStreamIdentity(request);
        BatchChunk chunk = request.getChunk();
        requireChunk(request, chunk);
        BatchKey key = new BatchKey(request.getPartitionId(), chunk.getBatchSequence());
        PartialBatch batch = partial.computeIfAbsent(key, ignored -> new PartialBatch(request));
        int chunkBytes = chunk.getData().size();
        if (bufferedBytes + chunkBytes > maxBatchBytes) {
            throw new IllegalArgumentException("write stream has too much incomplete data");
        }
        batch.append(request);
        bufferedBytes += chunkBytes;
        if (!batch.complete()) {
            return null;
        }
        partial.remove(key);
        bufferedBytes -= batch.size();
        return batch.finish();
    }

    /** Fails when the client half-closes with incomplete batches buffered. */
    public void requireComplete() {
        if (!partial.isEmpty()) {
            throw new IllegalStateException("write stream ended with an incomplete batch");
        }
    }

    private void requireStreamIdentity(WriteBatchRequest request) {
        if (request.getStreamSequence() != nextStreamSequence++) {
            throw new IllegalArgumentException("write stream sequence is not contiguous");
        }
        if (streamAttempt == null) {
            streamAttempt = request.getProducerAttempt();
            streamShuffleId = request.getShuffleId();
        } else if (!streamAttempt.equals(request.getProducerAttempt())
                || !streamShuffleId.equals(request.getShuffleId())) {
            throw new SecurityException("write stream changed producer identity");
        }
    }

    private void requireChunk(WriteBatchRequest request, BatchChunk chunk) {
        int dataSize = chunk.getData().size();
        if (!validRequestHeader(request, chunk)
                || !validChunkSizing(request, chunk, dataSize)) {
            throw new IllegalArgumentException("write batch chunk is invalid or unsupported");
        }
        byte[] chunkChecksum = ShuffleChecksums.requireSha256(
                chunk.getChunkChecksum(), "chunk checksum");
        if (!MessageDigest.isEqual(chunkChecksum,
                ShuffleChecksums.sha256(chunk.getData().toByteArray()))) {
            throw new ShuffleServiceFailure(FailureKind.CORRUPT,
                    "write chunk checksum does not match payload");
        }
        ShuffleChecksums.requireSha256(chunk.getBatchChecksum(), "batch checksum");
    }

    private boolean validRequestHeader(WriteBatchRequest request, BatchChunk chunk) {
        return !request.getShuffleId().isBlank() && request.getPartitionId() >= 0
                && request.getRowCount() > 0
                && request.getCodec() == CompressionCodec.COMPRESSION_CODEC_NONE
                && chunk.getBatchSequence() >= 0 && chunk.getChunkCount() > 0
                && chunk.getChunkIndex() < chunk.getChunkCount();
    }

    private boolean validChunkSizing(
            WriteBatchRequest request,
            BatchChunk chunk,
            int dataSize) {
        return dataSize > 0 && dataSize <= maxChunkBytes
                && chunk.getBatchSizeBytes() > 0
                && chunk.getBatchSizeBytes() <= maxBatchBytes
                && request.getUncompressedSize() == chunk.getBatchSizeBytes();
    }

    private record BatchKey(int partitionId, long sequence) {}

    private static final class PartialBatch {
        private final WriteBatchRequest first;
        private final ByteArrayOutputStream data;
        private int nextChunk;

        private PartialBatch(WriteBatchRequest first) {
            this.first = first;
            this.data = new ByteArrayOutputStream();
        }

        private void append(WriteBatchRequest request) {
            BatchChunk expected = first.getChunk();
            BatchChunk actual = request.getChunk();
            if (!sameChunkShape(actual, expected)
                    || !sameRequestMetadata(request)) {
                throw new IllegalArgumentException("batch chunks disagree on immutable metadata");
            }
            byte[] chunk = actual.getData().toByteArray();
            data.write(chunk, 0, chunk.length);
            nextChunk++;
            if (data.size() > expected.getBatchSizeBytes()) {
                throw new IllegalArgumentException("batch chunks exceed declared size");
            }
        }

        private boolean sameRequestMetadata(WriteBatchRequest request) {
            return request.getUncompressedSize() == first.getUncompressedSize()
                    && request.getRowCount() == first.getRowCount()
                    && request.getCodec() == first.getCodec()
                    && request.getAccessCapability().equals(first.getAccessCapability());
        }

        private boolean sameChunkShape(BatchChunk actual, BatchChunk expected) {
            return actual.getChunkIndex() == nextChunk
                    && actual.getChunkCount() == expected.getChunkCount()
                    && actual.getBatchSizeBytes() == expected.getBatchSizeBytes()
                    && actual.getBatchChecksum().equals(expected.getBatchChecksum());
        }

        private boolean complete() {
            return nextChunk == first.getChunk().getChunkCount();
        }

        private long size() {
            return data.size();
        }

        private CompleteWriteBatch finish() {
            byte[] canonical = data.toByteArray();
            byte[] expected = ShuffleChecksums.requireSha256(
                    first.getChunk().getBatchChecksum(), "batch checksum");
            if (canonical.length != first.getChunk().getBatchSizeBytes()
                    || !MessageDigest.isEqual(ShuffleChecksums.sha256(canonical), expected)) {
                throw new ShuffleServiceFailure(FailureKind.CORRUPT,
                        "complete batch does not match declared size or checksum");
            }
            return new CompleteWriteBatch(first.getShuffleId(), first.getPartitionId(),
                    first.getChunk().getBatchSequence(), first.getRowCount(),
                    first.getProducerAttempt(), first.getAccessCapability(), canonical, expected);
        }
    }
}
