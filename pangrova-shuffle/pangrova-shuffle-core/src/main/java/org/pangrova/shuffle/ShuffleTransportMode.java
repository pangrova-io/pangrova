package org.pangrova.shuffle;

import org.pangrova.spi.annotation.Experimental;

/**
 * Transport mode for shuffle data exchange between map and reduce stages (ADR-002 D2).
 *
 * <p>Determines how shuffle data flows from producers (map tasks) to consumers (reduce tasks):
 *
 * <ul>
 *   <li>{@link #PULL} — Consumers pull data from producers. The producer holds data in-process
 *       (typically in memory via Arrow Flight) and downstream reducers fetch it on demand.
 *       This is the current native Arrow Flight model. Shuffle lifetime is tied to the
 *       producing task — once the producer finishes, shuffle data is destroyed.</li>
 *
 *   <li>{@link #PUSH} — Producers push data to an external shuffle service (RSS) as batches
 *       are produced. The RSS aggregates, merges, and serves data independently of producer
 *       lifecycle. This decouples shuffle data lifetime from compute, enabling safe task
 *       retries and worker decommission without data loss. Aligns with Celeborn/Uniffle
 *       push-shuffle semantics.</li>
 * </ul>
 *
 * <p>The scheduler and {@link ShuffleManagerFactory} use this mode to select the appropriate
 * data-plane contract: PULL uses the in-worker {@link ShuffleTransport} SPI, while PUSH
 * uses the extended {@code ExchangeSink.write(mapId, attemptId, partitionId, data)} contract
 * that maps to Celeborn {@code pushData} or object-store PUT operations.</p>
 *
 * <p>Industry references: Spark Celeborn/Uniffle (push), Trino (pull-from-worker),
 * Magnet (push-based merge).</p>
 *
 * @see ShuffleCapabilities#transportMode()
 * @see ShuffleManagerFactory
 */
@Experimental(reason = "ADR-002 D2: push-style shuffle transport for storage-compute separation")
public enum ShuffleTransportMode {

    /**
     * Pull-from-producer model: consumers fetch shuffle data from the producing worker.
     * Shuffle data lifetime is tied to the producer's process lifetime.
     * Current default for the native Arrow Flight shuffle.
     */
    PULL,

    /**
     * Push-to-service model: producers push shuffle data to an external Remote Shuffle Service.
     * Shuffle data lifetime is independent of the producer — survives task completion,
     * worker decommission, and process crashes (for durable providers).
     * Used by Celeborn, Uniffle, and object-store shuffle providers.
     */
    PUSH
}
