package org.pangrova.shuffle.api.capability;

/**
 * Durability tier a {@link ShuffleProvider} guarantees for committed shuffle partitions
 * (ADR-002 D3). The scheduler binds task-retry semantics to this tier via
 * {@link ShuffleCapabilities#supportsTaskRetry()} — it never reads the provider name.
 */
public enum ShuffleDurability {

    /** In-memory only; lost if the producing worker crashes (today's native Arrow Flight). */
    EPHEMERAL,

    /** Worker local disk; survives task failure but not worker loss (in-tree FTE default). */
    LOCAL_SPILL,

    /** External RSS / object store; survives worker crash (Celeborn / Uniffle / object-store). */
    DURABLE
}
