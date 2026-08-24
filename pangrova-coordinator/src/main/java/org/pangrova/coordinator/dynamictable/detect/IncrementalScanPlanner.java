package org.pangrova.coordinator.dynamictable.detect;

/**
 * Plans an incremental scan between two snapshots of a source table.
 *
 * <p>Implementations resolve the data files added and delete files
 * detected in the snapshot range, producing an {@link IncrementalScanPlan}
 * that the {@link SnapshotChangeDetector} uses to determine the
 * change type (append-only, full-refresh-required, or empty).</p>
 */
public interface IncrementalScanPlanner {

    /**
     * Produce a scan plan covering the snapshot range
     * ({@code fromSnapshot}, {@code toSnapshot}].
     *
     * @param tableName    fully qualified source table name
     * @param fromSnapshot exclusive lower-bound snapshot ID
     * @param toSnapshot   inclusive upper-bound snapshot ID
     * @return scan plan describing files in the range
     */
    IncrementalScanPlan plan(String tableName, long fromSnapshot, long toSnapshot);
}
