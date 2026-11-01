package org.pangrova.shuffle.service.transport;

import org.pangrova.protocol.v2.shuffle.PartitionWriteAck;

/** Mutable per-partition acknowledgement totals owned by one serialized write stream. */
public final class PartitionWriteAccumulator {
    private long batches;
    private long bytes;
    private long lastSequence = -1;

    public void accepted(long sequence, long sizeBytes) {
        batches++;
        bytes = Math.addExact(bytes, sizeBytes);
        lastSequence = sequence;
    }

    public long batches() {
        return batches;
    }

    public long bytes() {
        return bytes;
    }

    public PartitionWriteAck protocol(int partition) {
        return PartitionWriteAck.newBuilder().setPartitionId(partition)
                .setBatchesAccepted(batches).setBytesAccepted(bytes)
                .setLastPartitionSequence(lastSequence).build();
    }
}
