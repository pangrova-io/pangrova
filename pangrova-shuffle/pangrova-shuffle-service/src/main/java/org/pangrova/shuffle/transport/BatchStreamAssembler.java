package org.pangrova.shuffle.transport;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.shuffle.checksum.ShuffleChecksums;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

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
        byte[] chunkChecksum = requireChunk(request, chunk);
        BatchKey key = new BatchKey(request.getPartitionId(), chunk.getBatchSequence());
        int chunkBytes = chunk.getData().size();
        if (bufferedBytes + chunkBytes > maxBatchBytes) {
            throw new IllegalArgumentException("write stream has too much incomplete data");
        }
        if (chunk.getChunkCount() == 1 && !partial.containsKey(key)) {
            if (chunkBytes > chunk.getBatchSizeBytes()) {
                throw new IllegalArgumentException("batch chunks exceed declared size");
            }
            return complete(request, chunk.getData(), chunkChecksum);
        }
        PartialBatch batch = partial.computeIfAbsent(key, ignored -> new PartialBatch(request));
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

    /** Drops the staged payload when the owning RPC has stopped processing chunks. */
    public void discard() {
        partial.clear();
        bufferedBytes = 0;
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

    private byte[] requireChunk(WriteBatchRequest request, BatchChunk chunk) {
        int dataSize = chunk.getData().size();
        if (!validRequestHeader(request, chunk)
                || !validChunkSizing(request, chunk, dataSize)) {
            throw new IllegalArgumentException("write batch chunk is invalid or unsupported");
        }
        byte[] chunkChecksum = ShuffleChecksums.requireSha256(
                chunk.getChunkChecksum(), "chunk checksum");
        if (!MessageDigest.isEqual(chunkChecksum, ShuffleChecksums.sha256(chunk.getData()))) {
            throw new ShuffleServiceException(FailureKind.CORRUPT,
                    "write chunk checksum does not match payload");
        }
        ShuffleChecksums.requireSha256(chunk.getBatchChecksum(), "batch checksum");
        return chunkChecksum;
    }

    private boolean validRequestHeader(WriteBatchRequest request, BatchChunk chunk) {
        return !request.getShuffleId().isBlank() && request.getPartitionId() >= 0
                && request.getRowCount() > 0
                && request.getCodec() == CompressionCodec.COMPRESSION_CODEC_NONE
                && chunk.getBatchSequence() >= 0 && chunk.getChunkCount() > 0
                && chunk.getChunkIndex() >= 0 && chunk.getChunkIndex() < chunk.getChunkCount();
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

    private static CompleteWriteBatch complete(
            WriteBatchRequest request, ByteString canonical, byte[] actualChecksum) {
        BatchChunk chunk = request.getChunk();
        byte[] expected = ShuffleChecksums.requireSha256(chunk.getBatchChecksum(), "batch checksum");
        if (canonical.size() != chunk.getBatchSizeBytes() || !MessageDigest.isEqual(actualChecksum, expected)) {
            throw new ShuffleServiceException(FailureKind.CORRUPT,
                    "complete batch does not match declared size or checksum");
        }
        return new CompleteWriteBatch(request.getShuffleId(), request.getPartitionId(),
                chunk.getBatchSequence(), request.getRowCount(),
                request.getProducerAttempt(), request.getAccessCapability(), canonical, expected);
    }

    private static final class PartialBatch {
        private final WriteBatchRequest first;
        private final ByteString.Output data;
        private int nextChunk;

        private PartialBatch(WriteBatchRequest first) {
            this.first = first;
            this.data = ByteString.newOutput(first.getChunk().getData().size());
        }

        private void append(WriteBatchRequest request) {
            BatchChunk expected = first.getChunk();
            BatchChunk actual = request.getChunk();
            if (!sameChunkShape(actual, expected)
                    || !sameRequestMetadata(request)) {
                throw new IllegalArgumentException("batch chunks disagree on immutable metadata");
            }
            ByteString chunk = actual.getData();
            if ((long) data.size() + chunk.size() > expected.getBatchSizeBytes()) {
                throw new IllegalArgumentException("batch chunks exceed declared size");
            }
            // ByteString.writeTo(OutputStream) first materializes a defensive array.
            try (var input = chunk.newInput()) {
                input.transferTo(data);
            } catch (IOException failure) {
                throw new IllegalStateException("in-memory shuffle batch staging failed", failure);
            }
            nextChunk++;
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
            ByteString canonical = data.toByteString();
            return BatchStreamAssembler.complete(first, canonical, ShuffleChecksums.sha256(canonical));
        }
    }
}
