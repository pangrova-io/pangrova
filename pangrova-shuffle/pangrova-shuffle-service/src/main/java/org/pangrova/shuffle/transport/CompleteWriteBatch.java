package org.pangrova.shuffle.transport;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;

public record CompleteWriteBatch(
        String shuffleId,
        int outputPartitionId,
        long batchSequence,
        long rowCount,
        TaskAttemptId attempt,
        ShuffleAccessCapability capability,
        byte[] canonicalBytes,
        byte[] checksum) {
    public CompleteWriteBatch {
        canonicalBytes = canonicalBytes.clone();
        checksum = checksum.clone();
    }

    @Override
    public byte[] canonicalBytes() {
        return canonicalBytes.clone();
    }

    /** Number of immutable batch bytes without materializing another payload copy. */
    public int canonicalSize() {
        return canonicalBytes.length;
    }

    /** Opens an independent stream over the owned batch for S3 request retries. */
    public InputStream openCanonicalStream() {
        return new ByteArrayInputStream(canonicalBytes);
    }

    @Override
    public byte[] checksum() {
        return checksum.clone();
    }
}
