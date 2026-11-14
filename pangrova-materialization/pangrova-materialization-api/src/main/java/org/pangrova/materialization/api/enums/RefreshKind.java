package org.pangrova.materialization.api.enums;

import org.pangrova.materialization.api.identity.StrategyId;

/**
 * Resolved execution kind of one refresh.
 *
 * <p>The kind is an explicit decision made before reservation. It is persisted
 * on the attempt and generation so that publication, reconciliation, and
 * garbage collection can reason about the exact computation that ran.
 */
public enum RefreshKind {
    /** Full recomputation of the materialized result. */
    FULL(StrategyId.FULL),
    /** Append-only incremental merge. */
    INCREMENTAL_APPEND_ONLY(StrategyId.APPEND_ONLY),
    /** Replacement of affected partitions. */
    INCREMENTAL_PARTITION_REPLACE(StrategyId.PARTITION_REPLACE),
    /** Recomputation of affected aggregation keys. */
    INCREMENTAL_AFFECTED_KEY(StrategyId.AFFECTED_KEY),
    /** Application of an ordered change-data stream. */
    INCREMENTAL_CDC(StrategyId.CDC);

    private final StrategyId strategyId;

    RefreshKind(StrategyId strategyId) {
        this.strategyId = strategyId;
    }

    public StrategyId strategyId() {
        return strategyId;
    }

    public boolean isIncremental() {
        return this != FULL;
    }

    public boolean satisfies(RefreshMode mode) {
        return switch (mode) {
            case FULL -> !isIncremental();
            case INCREMENTAL -> isIncremental();
        };
    }
}
