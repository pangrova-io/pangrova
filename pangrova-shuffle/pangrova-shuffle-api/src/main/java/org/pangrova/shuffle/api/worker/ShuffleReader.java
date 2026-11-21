package org.pangrova.shuffle.api.worker;

import org.pangrova.common.api.annotation.Experimental;

/** One authorized partition of a published exchange bound to a particular consumer attempt. */
@Experimental(reason = "Partition reader lifecycle")
public interface ShuffleReader<C> extends AutoCloseable {
    /** Waits for data or failure. Null acknowledges verified publication and terminal evidence. */
    C nextVerifiedChunk();

    /** Last full verified batch sequence, or -1 before the first complete batch. */
    long lastCompletedSequence();

    /** Stops outstanding reads and wakes the reader without releasing a still-borrowed buffer. */
    void revoke();

    /** Joins pending reads and releases leases. A failed drain remains owned and retryable. */
    @Override void close();
}
