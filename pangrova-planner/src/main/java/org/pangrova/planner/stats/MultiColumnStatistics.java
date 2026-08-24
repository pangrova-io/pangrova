package org.pangrova.planner.stats;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Statistics for a correlated group of columns, modelled after PostgreSQL extended statistics.
 * Captures the combined number-of-distinct-values of the column tuple ({@code n-distinct}) and
 * any soft {@link FunctionalDependency functional dependencies} among the group. Used by the CBO
 * to correct the naive independence assumption when estimating conjunctive-predicate selectivity
 * over correlated columns (which otherwise severely under-estimates cardinality).
 *
 * @param columns      the column group, in canonical order; at least two, no duplicates
 * @param rowCount     total rows the statistic was computed over
 * @param combinedNdv  number of distinct value-combinations across {@code columns} (≥ 0; 0 = unknown)
 * @param dependencies soft functional dependencies among {@code columns}; may be empty
 */
public record MultiColumnStatistics(
        List<String> columns,
        long rowCount,
        long combinedNdv,
        List<FunctionalDependency> dependencies) {

    public MultiColumnStatistics {
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.size() < 2) {
            throw new IllegalArgumentException("multi-column statistics require at least 2 columns");
        }
        if (new HashSet<>(columns).size() != columns.size()) {
            throw new IllegalArgumentException("columns must not contain duplicates: " + columns);
        }
        if (combinedNdv < 0) {
            throw new IllegalArgumentException("combinedNdv must be >= 0, got " + combinedNdv);
        }
        columns = List.copyOf(columns);
        dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
        for (FunctionalDependency d : dependencies) {
            if (!columns.contains(d.dependent()) || !columns.containsAll(d.determinants())) {
                throw new IllegalArgumentException(
                        "dependency " + d + " references columns outside the group " + columns);
            }
        }
    }

    /** @return true when every predicate column is part of this column group. */
    public boolean covers(Collection<String> predicateColumns) {
        return columns.containsAll(predicateColumns);
    }

    /** @return true when a usable n-distinct estimate is available. */
    public boolean hasNdistinct() {
        return combinedNdv > 0;
    }
}
