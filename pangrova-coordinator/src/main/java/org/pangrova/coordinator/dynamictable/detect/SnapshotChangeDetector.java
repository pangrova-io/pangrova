package org.pangrova.coordinator.dynamictable.detect;

import org.pangrova.metadata.dynamictable.DynamicTableState;

import java.util.Objects;

/**
 * Detects source-table changes via snapshot diff.
 *
 * <p>Delegates to a {@link IncrementalScanPlanner} to obtain the file
 * list between the last checkpoint and the current snapshot, then
 * classifies the result:</p>
 * <ul>
 *   <li>Delete files present → {@link ChangeResult#fullRefreshRequired}</li>
 *   <li>No data files → {@link ChangeResult#empty()}</li>
 *   <li>Data files only → {@link ChangeResult#incremental}</li>
 * </ul>
 */
public final class SnapshotChangeDetector implements ChangeDetector {

    private final IncrementalScanPlanner planner;

    public SnapshotChangeDetector(IncrementalScanPlanner planner) {
        this.planner = Objects.requireNonNull(planner, "planner must not be null");
    }

    @Override
    public ChangeResult detect(DynamicTableState state) {
        Objects.requireNonNull(state, "state must not be null");

        long fromSnapshot = parseCheckpoint(state.checkpoint());
        // toSnapshot = 0 signals "resolve to current snapshot" to the planner
        IncrementalScanPlan plan = planner.plan(state.dtName(), fromSnapshot, 0L);

        if (!plan.deleteFiles().isEmpty()) {
            return ChangeResult.fullRefreshRequired(plan);
        }
        if (plan.dataFiles().isEmpty()) {
            return ChangeResult.empty();
        }
        return ChangeResult.incremental(plan);
    }

    private static long parseCheckpoint(String checkpoint) {
        if (checkpoint == null || checkpoint.isBlank()) {
            return 0L;
        }
        return Long.parseLong(checkpoint);
    }
}
