package org.pangrova.coordinator.dynamictable.refresh.pipeline;

import org.pangrova.coordinator.dynamictable.detect.ChangeDetector;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.IncrementalEvaluator;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.DynamicTableState;

import java.util.Objects;

/**
 * Sealed pipeline abstraction for Dynamic Table refresh strategies.
 *
 * <p>Encapsulates the detect → evaluate two-phase pipeline, delegating
 * to the appropriate {@link ChangeDetector} and {@link IncrementalEvaluator}
 * based on the refresh mode (SNAPSHOT or CDC).</p>
 *
 * <p>Both SNAPSHOT and CDC pipelines share identical detect/evaluate logic;
 * the {@link #refreshMode()} discriminator allows callers to distinguish
 * pipeline type without {@code instanceof} checks.</p>
 */
public sealed interface RefreshPipeline
        permits RefreshPipeline.SnapshotRefreshPipeline,
                RefreshPipeline.CdcRefreshPipeline {

    /** The refresh mode this pipeline operates in. */
    DynamicTableRefreshMode refreshMode();

    /** Access the underlying change detector. */
    ChangeDetector detector();

    /** Access the underlying incremental evaluator. */
    IncrementalEvaluator evaluator();

    /**
     * Detect changes in the source table since the last checkpoint.
     *
     * @param state current Dynamic Table state
     * @return change detection result
     */
    default ChangeResult detectChanges(DynamicTableState state) {
        return detector().detect(state);
    }

    /**
     * Evaluate detected changes through the defining query logic.
     *
     * @param state   current Dynamic Table state
     * @param changes detected changes from {@link #detectChanges}
     * @return evaluation result with output rows
     */
    default IncrementalResult evaluate(DynamicTableState state, ChangeResult changes) {
        return evaluator().evaluate(state, changes);
    }

    /**
     * Snapshot-based refresh pipeline: snapshot diff detection + incremental evaluation.
     */
    record SnapshotRefreshPipeline(
            ChangeDetector detector,
            IncrementalEvaluator evaluator
    ) implements RefreshPipeline {

        public SnapshotRefreshPipeline {
            Objects.requireNonNull(detector, "detector must not be null");
            Objects.requireNonNull(evaluator, "evaluator must not be null");
        }

        @Override
        public DynamicTableRefreshMode refreshMode() {
            return DynamicTableRefreshMode.SNAPSHOT;
        }
    }

    /**
     * CDC-based refresh pipeline: CDC stream consumption + incremental evaluation.
     */
    record CdcRefreshPipeline(
            ChangeDetector detector,
            IncrementalEvaluator evaluator
    ) implements RefreshPipeline {

        public CdcRefreshPipeline {
            Objects.requireNonNull(detector, "detector must not be null");
            Objects.requireNonNull(evaluator, "evaluator must not be null");
        }

        @Override
        public DynamicTableRefreshMode refreshMode() {
            return DynamicTableRefreshMode.CDC;
        }
    }
}
