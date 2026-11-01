package org.pangrova.spi.indexing.statistics;

import org.pangrova.common.api.annotation.Experimental;

import java.util.BitSet;
import java.util.List;

/**
 * Describes which row groups within a file should be read or skipped
 * after row-group-level predicate evaluation.
 *
 * <p>Produced by {@code RowGroupPruner} and consumed by the scan path
 * to avoid reading row groups whose statistics prove they contain no
 * matching rows.</p>
 *
 * @param filePath        data file this skip info applies to
 * @param totalRowGroups  total number of row groups in the file
 * @param skippedGroups   bit set where bit {@code i} is set if row group {@code i} should be skipped
 */
@Experimental
public record RowGroupSkipInfo(
        String filePath,
        int totalRowGroups,
        BitSet skippedGroups) {

    /** No row groups skipped — read everything. */
    public static final RowGroupSkipInfo NONE = new RowGroupSkipInfo("", 0, new BitSet());

    public RowGroupSkipInfo {
        skippedGroups = skippedGroups == null ? new BitSet() : (BitSet) skippedGroups.clone();
    }

    /** Returns the number of row groups that will be skipped. */
    public int skippedCount() {
        return skippedGroups.cardinality();
    }

    /** Returns the number of row groups that will be read. */
    public int retainedCount() {
        return totalRowGroups - skippedCount();
    }

    /** Returns true if at least one row group is skipped. */
    public boolean hasSkips() {
        return skippedCount() > 0;
    }

    /** Returns true if the given row group index should be read (not skipped). */
    public boolean shouldRead(int rowGroupIndex) {
        return !skippedGroups.get(rowGroupIndex);
    }

    /** Returns the indices of row groups that should be read. */
    public List<Integer> retainedIndices() {
        var indices = new java.util.ArrayList<Integer>();
        for (int i = 0; i < totalRowGroups; i++) {
            if (!skippedGroups.get(i)) {
                indices.add(i);
            }
        }
        return List.copyOf(indices);
    }
}
