package org.pangrova.worker.exchange.remote;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.shuffle.api.commit.ShufflePartitionManifestChecksum;
import org.pangrova.worker.exchange.CanonicalBatchChunks;

/** Ordered producer statistics and digest for one output partition. */
final class RemoteShufflePartitionStats {

    private final ShufflePartitionManifestChecksum.Accumulator manifest =
            ShufflePartitionManifestChecksum.accumulator();
    private final ReentrantLock lock = new ReentrantLock();
    private long batches;
    private long bytes;
    private long rows;

    long nextSequence() {
        return batches;
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    void record(long rowCount, ByteBuffer data) {
        int byteCount = data.remaining();
        byte[] payloadChecksum = CanonicalBatchChunks.sha256(data)
                .getDigest().toByteArray();
        manifest.add(batches, byteCount, rowCount, payloadChecksum);
        batches++;
        bytes += byteCount;
        rows = Math.addExact(rows, rowCount);
    }

    long bytes() {
        return bytes;
    }

    PartitionCommitSummary summary(int partitionId) {
        return PartitionCommitSummary.newBuilder()
                .setPartitionId(partitionId)
                .setExpectedLastSequence(batches - 1)
                .setOutputChecksum(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(manifest.finishNonEmpty())))
                .setTotalBytes(bytes)
                .setTotalBatches(batches)
                .setTotalRows(rows)
                .build();
    }
}
