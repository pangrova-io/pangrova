package org.pangrova.shuffle;

import java.time.Duration;

import org.pangrova.spi.annotation.Experimental;

/**
 * Handles draining of shuffle data on a worker during graceful shutdown or decommission (G-RES-08).
 *
 * <p>When a worker receives a decommission signal, the shutdown sequence invokes this handler
 * to ensure committed shuffle data is either:
 * <ul>
 *   <li>Persisted to durable storage (for DETACHED/PERSISTENT providers), or</li>
 *   <li>Migrated to a peer worker (for EPHEMERAL providers, best-effort), or</li>
 *   <li>Reported as lost so the coordinator can schedule re-execution.</li>
 * </ul>
 *
 * <p>Implementations are tied to the shuffle provider: a local-spill provider flushes buffers
 * to disk; an object-store provider ensures pending uploads complete; a native (EPHEMERAL)
 * provider reports all data as at risk.
 */
@Experimental(reason = "G-RES-08: shuffle drain for safe worker decommission")
public interface ShuffleDrainHandler {

    /**
     * Result of a drain attempt.
     *
     * @param drained      number of shuffle partitions successfully drained/persisted
     * @param atRisk       number of shuffle partitions that could not be drained (will be lost)
     * @param bytesAtRisk  estimated bytes of data that will be lost
     */
    record DrainResult(int drained, int atRisk, long bytesAtRisk) {

        /** All data was successfully drained. */
        public static final DrainResult ALL_DRAINED = new DrainResult(0, 0, 0);

        public DrainResult {
            if (drained < 0) {
                throw new IllegalArgumentException("drained must be non-negative");
            }
            if (atRisk < 0) {
                throw new IllegalArgumentException("atRisk must be non-negative");
            }
            if (bytesAtRisk < 0) {
                throw new IllegalArgumentException("bytesAtRisk must be non-negative");
            }
        }

        /** Returns true if all data was drained with nothing at risk. */
        public boolean isFullyDrained() {
            return atRisk == 0;
        }
    }

    /**
     * Attempts to drain all committed shuffle data within the given timeout.
     *
     * <p>For PERSISTENT providers, this is typically a no-op (data is already durable).
     * For DETACHED providers, this flushes pending writes to local persistent storage.
     * For EPHEMERAL providers, this reports all live data as at risk.
     *
     * @param timeout maximum time to wait for drain to complete
     * @return the drain result
     */
    DrainResult drain(Duration timeout);

    /**
     * Returns the retention policy this handler operates under.
     */
    ShuffleRetentionPolicy retentionPolicy();

    /**
     * A no-op drain handler for providers where data is already persistent.
     * Returns {@link DrainResult#ALL_DRAINED} immediately.
     */
    static ShuffleDrainHandler persistent() {
        return new ShuffleDrainHandler() {
            @Override
            public DrainResult drain(Duration timeout) {
                return DrainResult.ALL_DRAINED;
            }

            @Override
            public ShuffleRetentionPolicy retentionPolicy() {
                return ShuffleRetentionPolicy.PERSISTENT;
            }
        };
    }
}
