package org.pangrova.materialization.domain;

/**
 * Resolved execution kind of one refresh.
 *
 * <p>The kind is an explicit decision made before reservation. It is persisted
 * on the attempt and generation so that publication, reconciliation, and
 * garbage collection can reason about the exact computation that ran.
 */
public sealed interface RefreshKind
        permits RefreshKind.Full, RefreshKind.IncrementalAppendOnly,
        RefreshKind.IncrementalPartitionReplace, RefreshKind.IncrementalAffectedKey,
        RefreshKind.IncrementalCdc {

    /**
     * Returns whether the kind applies a change set to an existing generation.
     *
     * @return true for incremental kinds
     */
    boolean isIncremental();

    /**
     * Returns the strategy identifier bound to this kind.
     *
     * @return strategy identifier
     */
    StrategyId strategyId();

    /** Full recomputation of the materialized result. */
    enum Full implements RefreshKind {
        INSTANCE;

        @Override
        public boolean isIncremental() {
            return false;
        }

        @Override
        public StrategyId strategyId() {
            return StrategyId.FULL;
        }
    }

    /** Append-only incremental merge. */
    enum IncrementalAppendOnly implements RefreshKind {
        INSTANCE;

        @Override
        public boolean isIncremental() {
            return true;
        }

        @Override
        public StrategyId strategyId() {
            return StrategyId.APPEND_ONLY;
        }
    }

    /** Replacement of affected partitions. */
    enum IncrementalPartitionReplace implements RefreshKind {
        INSTANCE;

        @Override
        public boolean isIncremental() {
            return true;
        }

        @Override
        public StrategyId strategyId() {
            return StrategyId.PARTITION_REPLACE;
        }
    }

    /** Recomputation of affected aggregation keys. */
    enum IncrementalAffectedKey implements RefreshKind {
        INSTANCE;

        @Override
        public boolean isIncremental() {
            return true;
        }

        @Override
        public StrategyId strategyId() {
            return StrategyId.AFFECTED_KEY;
        }
    }

    /** Application of an ordered change-data stream. */
    enum IncrementalCdc implements RefreshKind {
        INSTANCE;

        @Override
        public boolean isIncremental() {
            return true;
        }

        @Override
        public StrategyId strategyId() {
            return StrategyId.CDC;
        }
    }
}
