package org.pangrova.worker.exchange.remote;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.shuffle.core.ShufflePartitionManifestChecksum;
import org.pangrova.worker.exchange.CanonicalBatchChunks;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Incrementally verifies ordered chunks without materializing a complete batch. */
final class StreamingBatchChunkValidator {

    private final ShufflePartitionManifestChecksum.Accumulator manifest =
            ShufflePartitionManifestChecksum.accumulator();
    private MessageDigest batchDigest;
    private Checksum expectedBatchChecksum;
    private long nextBatchSequence;
    private long batchSize;
    private long batchBytes;
    private long totalBytes;
    private long totalRows;
    private long batchRows;
    private int nextChunkIndex;
    private int chunkCount;

    void accept(BatchChunk chunk, long rowCount) {
        validateIdentity(chunk);
        if (chunk.getChunkIndex() == 0) {
            beginBatch(chunk, rowCount);
        }
        validateCurrentBatch(chunk, rowCount);
        ByteString data = chunk.getData();
        int expectedLength = (int) Math.min(
                CanonicalBatchChunks.MAX_CHUNK_BYTES, batchSize - batchBytes);
        if (data.size() != expectedLength
                || !matchesChecksum(chunk.getChunkChecksum(), data)) {
            throw corruption("chunk size or checksum is invalid");
        }
        batchDigest.update(data.asReadOnlyByteBuffer());
        batchBytes += data.size();
        totalBytes += data.size();
        nextChunkIndex++;
        if (nextChunkIndex == chunkCount) {
            completeBatch();
        }
    }

    boolean hasPartialBatch() {
        return batchDigest != null;
    }

    long nextBatchSequence() {
        return nextBatchSequence;
    }

    long totalBytes() {
        return totalBytes;
    }

    long totalRows() {
        return totalRows;
    }

    void validateOutputChecksum(Checksum expected) {
        if (!matchesDigest(expected, manifest.finish())) {
            throw corruption("partition output checksum is invalid");
        }
    }

    private void validateIdentity(BatchChunk chunk) {
        if (chunk.getBatchSequence() != nextBatchSequence
                || chunk.getChunkIndex() != nextChunkIndex
                || chunk.getChunkCount() == 0
                || chunk.getBatchSizeBytes() == 0
                || chunk.getBatchSizeBytes() > CanonicalBatchChunks.MAX_BATCH_BYTES) {
            throw corruption("chunk identity or batch bound is invalid");
        }
    }

    private void beginBatch(BatchChunk chunk, long rowCount) {
        if (batchDigest != null) {
            throw corruption("new batch started before the previous batch completed");
        }
        long size = chunk.getBatchSizeBytes();
        long expectedChunks = (size + CanonicalBatchChunks.MAX_CHUNK_BYTES - 1)
                / CanonicalBatchChunks.MAX_CHUNK_BYTES;
        if (rowCount <= 0
                || chunk.getChunkCount() != expectedChunks
                || !validSha256(chunk.getBatchChecksum())) {
            throw corruption("batch chunk count or checksum contract is invalid");
        }
        batchDigest = sha256Digest();
        expectedBatchChecksum = chunk.getBatchChecksum();
        batchSize = size;
        batchBytes = 0;
        batchRows = rowCount;
        chunkCount = chunk.getChunkCount();
    }

    private void validateCurrentBatch(BatchChunk chunk, long rowCount) {
        if (batchDigest == null
                || chunk.getChunkCount() != chunkCount
                || chunk.getBatchSizeBytes() != batchSize
                || rowCount != batchRows
                || !chunk.getBatchChecksum().equals(expectedBatchChecksum)) {
            throw corruption("chunk metadata changed within a batch");
        }
    }

    private void completeBatch() {
        byte[] payloadChecksum = batchDigest.digest();
        if (batchBytes != batchSize
                || !matchesDigest(expectedBatchChecksum, payloadChecksum)) {
            throw corruption("reassembled batch checksum is invalid");
        }
        manifest.add(nextBatchSequence, batchSize, batchRows, payloadChecksum);
        totalRows = Math.addExact(totalRows, batchRows);
        batchDigest = null;
        expectedBatchChecksum = null;
        batchSize = 0;
        batchBytes = 0;
        batchRows = 0;
        nextChunkIndex = 0;
        chunkCount = 0;
        nextBatchSequence++;
    }

    private static boolean matchesChecksum(Checksum checksum, ByteString data) {
        MessageDigest digest = sha256Digest();
        digest.update(data.asReadOnlyByteBuffer());
        return matchesDigest(checksum, digest.digest());
    }

    private static boolean matchesDigest(Checksum checksum, byte[] digest) {
        return validSha256(checksum) && MessageDigest.isEqual(
                checksum.getDigest().toByteArray(), digest);
    }

    private static boolean validSha256(Checksum checksum) {
        return checksum.getAlgorithm() == Checksum.Algorithm.ALGORITHM_SHA256
                && checksum.getDigest().size() == 32;
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static RemoteShuffleClientException corruption(String message) {
        return new RemoteShuffleClientException(
                "External shuffle corruption: " + message);
    }
}
