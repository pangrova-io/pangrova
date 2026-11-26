package org.pangrova.shuffle.transport;

import java.io.InputStream;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;

public record CompleteWriteBatch(
        String shuffleId,
        int outputPartitionId,
        long batchSequence,
        long rowCount,
        TaskAttemptId attempt,
        ShuffleAccessCapability capability,
        ByteString canonicalBytes,
        byte[] checksum) {
    public CompleteWriteBatch {
        checksum = checksum.clone();
    }

    /** Number of immutable batch bytes without materializing another payload copy. */
    public int canonicalSize() {
        return canonicalBytes.size();
    }

    /** Opens an independent stream over the owned batch for S3 request retries. */
    public InputStream openCanonicalStream() {
        return canonicalBytes.newInput();
    }

    @Override
    public byte[] checksum() {
        return checksum.clone();
    }
}
