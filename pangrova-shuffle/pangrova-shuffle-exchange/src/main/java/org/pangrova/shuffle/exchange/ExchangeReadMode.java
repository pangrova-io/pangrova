package org.pangrova.shuffle.exchange;

/**
 * Controls data visibility semantics for an {@link ExchangeSource}.
 *
 * <p>This is a data-layer contract, not a scheduling concept. The scheduling
 * layer (DAGDispatchEngine) uses a single unified rule: schedule downstream
 * when upstream reaches RUNNING. The {@code ExchangeReadMode} determines
 * when the downstream can actually <em>read</em> data from the exchange.</p>
 *
 * <p>Mapping from distribution type is deterministic:</p>
 * <ul>
 *   <li>HASH_DISTRIBUTED, RANGE_DISTRIBUTED → {@link #BLOCKING}</li>
 *   <li>BROADCAST_DISTRIBUTED, SINGLETON → {@link #STREAMING}</li>
 * </ul>
 */
public enum ExchangeReadMode {

    /**
     * Data is available only after upstream commits (finishes).
     * {@link ExchangeSource#getNextNativePage()} returns {@code null}
     * until the upstream stage signals commit.
     * Used for hash/range distribution where partition completeness is required.
     */
    BLOCKING,

    /**
     * Data flows as soon as upstream writes it.
     * {@link ExchangeSource#getNextNativePage()} returns data immediately
     * when available.
     * Used for broadcast/singleton distribution.
     */
    STREAMING
}
