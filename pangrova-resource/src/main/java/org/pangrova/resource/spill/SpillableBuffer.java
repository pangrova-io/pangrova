package org.pangrova.resource.spill;

/**
 * SPI interface for buffers that can be spilled to disk under memory pressure.
 *
 * <p>Implementations register themselves as {@link SpillManager.SpillCandidate}
 * instances via {@link SpillManager#registerCandidate}, retaining their live
 * reclaimable byte count and actual spill result. The SpillManager selects candidates by priority
 * and invokes the reclaim action synchronously during the allocation-time
 * reclaim handshake (Task 94/97) or the periodic pressure response.</p>
 *
 * <p>Priority convention (lower = spilled first):
 * <ol>
 *   <li>1 = ScanBuffer (read-side, cheap to re-read)</li>
 *   <li>2 = ShuffleBuffer / ExchangeBuffer (inter-node transfer buffers)</li>
 *   <li>3 = Sort buffer</li>
 *   <li>4 = Aggregation buffer</li>
 *   <li>5 = HashJoin build-side buffer (most expensive to rebuild)</li>
 * </ol>
 *
 * <p>Implementations must be thread-safe: {@link #spillToDisk} may be called
 * concurrently with normal buffer operations (add/poll). The implementation
 * should drain the oldest/largest partition data to local disk (NVMe preferred)
 * and release the corresponding memory reservation back to the arbitrator.</p>
 *
 * <p>Task 95 / G-MEM-03: this interface extends spill coverage to shuffle and
 * exchange buffers, which previously had no spill path and were unbounded.</p>
 *
 * @see SpillManager
 * @see SpillManager.SpillCandidate
 */
public interface SpillableBuffer {

    /**
     * Returns a unique identifier for this buffer instance (e.g.,
     * "shuffle-q123-stage5" or "exchange-q456-partition3").
     */
    String spillId();

    /**
     * Returns the spill priority of this buffer. Lower values are spilled
     * first under memory pressure.
     *
     * @see SpillManager.SpillCandidate#priority()
     */
    int spillPriority();

    /**
     * Returns the estimated number of bytes that can be reclaimed by
     * spilling this buffer's in-memory data to disk.
     */
    long reclaimableBytes();

    /**
     * Spills up to {@code targetBytes} of in-memory data to local disk,
     * releasing the corresponding memory reservation.
     *
     * <p>The implementation should:
     * <ol>
     *   <li>Serialize the oldest/largest partition batches to the configured
     *       spill directory</li>
     *   <li>Release the corresponding memory from the global arbitrator</li>
     *   <li>Return the actual number of bytes freed (may be less than
     *       {@code targetBytes} if the buffer has less data)</li>
     * </ol>
     *
     * @param targetBytes maximum bytes to spill (hint; actual may be less)
     * @return actual bytes freed from memory
     */
    long spillToDisk(long targetBytes);

    /**
     * Builds a {@link SpillManager.SpillCandidate} retaining this buffer as its owner,
     * suitable for registration with {@link SpillManager#registerCandidate}.
     * Register again when adding new data after the manager drains and removes the candidate.
     */
    default SpillManager.SpillCandidate toSpillCandidate() {
        return new SpillManager.SpillCandidate(
                this);
    }
}
