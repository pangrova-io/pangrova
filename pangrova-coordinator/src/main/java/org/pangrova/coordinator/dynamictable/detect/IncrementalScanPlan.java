package org.pangrova.coordinator.dynamictable.detect;

import java.util.List;
import java.util.Objects;

/**
 * Describes the incremental scan range between two snapshots.
 *
 * <p>Contains the data files added and delete files detected in the
 * snapshot range [{@code fromSnapshot}, {@code toSnapshot}]. Used by
 * {@link ChangeDetector} implementations to communicate what changed
 * in the source table since the last checkpoint.</p>
 *
 * @param dataFiles    newly added data file paths (immutable, never null)
 * @param deleteFiles  delete file paths detected in the range (immutable, never null)
 * @param fromSnapshot exclusive lower-bound snapshot ID
 * @param toSnapshot   inclusive upper-bound snapshot ID
 */
public record IncrementalScanPlan(
        List<String> dataFiles,
        List<String> deleteFiles,
        long fromSnapshot,
        long toSnapshot
) {

    public IncrementalScanPlan {
        Objects.requireNonNull(dataFiles, "dataFiles must not be null");
        Objects.requireNonNull(deleteFiles, "deleteFiles must not be null");
        dataFiles = List.copyOf(dataFiles);
        deleteFiles = List.copyOf(deleteFiles);
    }
}
