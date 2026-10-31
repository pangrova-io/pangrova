package org.pangrova.worker.exchange.codec;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.ByteBuffer;

/** Creates bounded chunks for the canonical serialized-batch contract. */
public final class CanonicalBatchChunks {

    public static final int MAX_CHUNK_BYTES = 1024 * 1024;
    public static final int MAX_BATCH_BYTES = 256 * 1024 * 1024;

    private CanonicalBatchChunks() {}

    /** Returns the exact number of one-MiB chunks required by a complete batch. */
    public static int chunkCount(byte[] batch) {
        requireBatch(batch);
        return chunkCount(batch.length);
    }

    /** Returns the exact chunk count for the buffer's remaining batch bytes. */
    public static int chunkCount(ByteBuffer batch) {
        requireBatch(batch);
        return chunkCount(batch.remaining());
    }

    /** Creates one checksummed chunk without copying the complete batch. */
    public static BatchChunk chunk(
            byte[] batch,
            long batchSequence,
            int chunkIndex,
            int chunkCount,
            Checksum batchChecksum) {
        requireBatch(batch);
        if (batchSequence < 0 || chunkIndex < 0 || chunkIndex >= chunkCount
                || chunkCount != chunkCount(batch)) {
            throw new IllegalArgumentException("Canonical batch chunk identity is invalid");
        }
        int offset = Math.multiplyExact(chunkIndex, MAX_CHUNK_BYTES);
        int length = Math.min(MAX_CHUNK_BYTES, batch.length - offset);
        ByteString data = ByteString.copyFrom(batch, offset, length);
        return BatchChunk.newBuilder()
                .setBatchSequence(batchSequence)
                .setChunkIndex(chunkIndex)
                .setChunkCount(chunkCount)
                .setBatchSizeBytes(batch.length)
                .setData(data)
                .setChunkChecksum(sha256(batch, offset, length))
                .setBatchChecksum(batchChecksum)
                .build();
    }

    /** Creates one chunk from a heap or direct buffer without copying the full batch. */
    public static BatchChunk chunk(
            ByteBuffer batch,
            long batchSequence,
            int chunkIndex,
            int chunkCount,
            Checksum batchChecksum) {
        requireBatch(batch);
        if (batchSequence < 0 || chunkIndex < 0 || chunkIndex >= chunkCount
                || chunkCount != chunkCount(batch)) {
            throw new IllegalArgumentException("Canonical batch chunk identity is invalid");
        }
        ByteBuffer view = batch.asReadOnlyBuffer();
        int batchSize = view.remaining();
        int offset = Math.multiplyExact(chunkIndex, MAX_CHUNK_BYTES);
        int length = Math.min(MAX_CHUNK_BYTES, batchSize - offset);
        view.position(view.position() + offset).limit(view.position() + length);
        ByteString data = ByteString.copyFrom(view.slice());
        return BatchChunk.newBuilder()
                .setBatchSequence(batchSequence)
                .setChunkIndex(chunkIndex)
                .setChunkCount(chunkCount)
                .setBatchSizeBytes(batchSize)
                .setData(data)
                .setChunkChecksum(sha256Remaining(data.asReadOnlyByteBuffer()))
                .setBatchChecksum(batchChecksum)
                .build();
    }

    /** Returns the canonical SHA-256 checksum of a complete batch. */
    public static Checksum sha256(byte[] data) {
        return sha256(data, 0, data.length);
    }

    /** Returns the canonical checksum without changing a direct buffer's position. */
    public static Checksum sha256(ByteBuffer data) {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        return sha256Remaining(data.asReadOnlyBuffer());
    }

    private static Checksum sha256(byte[] data, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(data, offset, length);
            return Checksum.newBuilder()
                    .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                    .setDigest(ByteString.copyFrom(digest.digest()))
                    .build();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static Checksum sha256Remaining(ByteBuffer data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(data);
            return checksum(digest.digest());
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static Checksum checksum(byte[] digest) {
        return Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(digest))
                .build();
    }

    private static int chunkCount(int batchBytes) {
        return Math.toIntExact(
                (batchBytes + (long) MAX_CHUNK_BYTES - 1) / MAX_CHUNK_BYTES);
    }

    private static void requireBatch(byte[] batch) {
        if (batch == null || batch.length == 0 || batch.length > MAX_BATCH_BYTES) {
            throw new IllegalArgumentException(
                    "Serialized batch must contain 1.." + MAX_BATCH_BYTES + " bytes");
        }
    }

    private static void requireBatch(ByteBuffer batch) {
        if (batch == null || !batch.hasRemaining()
                || batch.remaining() > MAX_BATCH_BYTES) {
            throw new IllegalArgumentException(
                    "Serialized batch must contain 1.." + MAX_BATCH_BYTES + " bytes");
        }
    }
}
