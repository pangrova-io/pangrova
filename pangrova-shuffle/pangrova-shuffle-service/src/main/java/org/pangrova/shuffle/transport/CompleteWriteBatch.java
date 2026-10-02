package org.pangrova.shuffle.transport;

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
        checksum = checksum.clone();
    }

    @Override
    public byte[] checksum() {
        return checksum.clone();
    }
}
