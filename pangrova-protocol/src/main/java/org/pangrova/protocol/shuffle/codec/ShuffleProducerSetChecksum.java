package org.pangrova.protocol.shuffle.codec;

import org.pangrova.common.crypto.Sha256Digests;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/** Computes the canonical identity of a stage's logical shuffle producers. */
public final class ShuffleProducerSetChecksum {

    private ShuffleProducerSetChecksum() {}

    /**
     * Hashes the sorted logical identities for partitions {@code [0, producerCount)}.
     * Retry-specific task and attempt identifiers are deliberately excluded.
     */
    public static byte[] forContiguousPartitions(
            String queryId, int stageId, long planEpoch, int producerCount) {
        checkNotNull(queryId, "queryId must not be null");
        if (queryId.isBlank() || stageId < 0 || planEpoch <= 0 || producerCount <= 0) {
            throw new IllegalArgumentException(
                    "producer-set identity requires query, stage, epoch, and producers");
        }
        MessageDigest digest = Sha256Digests.sha256();
        byte[] queryBytes = queryId.getBytes(StandardCharsets.UTF_8);
        updateInt(digest, producerCount);
        for (int partitionId = 0; partitionId < producerCount; partitionId++) {
            updateInt(digest, queryBytes.length);
            digest.update(queryBytes);
            updateInt(digest, stageId);
            updateInt(digest, partitionId);
            updateLong(digest, planEpoch);
        }
        return digest.digest();
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }
}
