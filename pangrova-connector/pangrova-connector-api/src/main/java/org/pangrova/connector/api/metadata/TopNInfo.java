package org.pangrova.connector.api.metadata;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.metadata.table.ColumnHandle;

/**
 * Describes a top-N pushdown request: the sort columns, their ordering,
 * and the maximum number of rows to return.
 *
 * <p>Connectors that support ordered scans with early termination can use
 * this to avoid reading the entire dataset when only the top-N rows are needed.</p>
 */
@Experimental
public record TopNInfo(List<SortColumn> sortColumns, long topN) {

    /**
     * A single sort column with direction.
     */
    public record SortColumn(ColumnHandle column, SortOrder order) {}

    /**
     * Sort direction.
     */
    public enum SortOrder {
        ASC_NULLS_FIRST,
        ASC_NULLS_LAST,
        DESC_NULLS_FIRST,
        DESC_NULLS_LAST
    }

    public TopNInfo {
        if (sortColumns == null || sortColumns.isEmpty()) {
            throw new IllegalArgumentException("sortColumns must not be null or empty");
        }
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be positive");
        }
        sortColumns = List.copyOf(sortColumns);
    }
}
