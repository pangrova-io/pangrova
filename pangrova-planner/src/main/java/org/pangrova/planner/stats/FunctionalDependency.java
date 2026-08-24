package org.pangrova.planner.stats;

import java.util.List;
import java.util.Objects;

/**
 * A soft functional dependency between columns of one table, modelled after PostgreSQL
 * extended-statistics {@code dependencies}. States that the {@code determinants} columns
 * functionally determine the {@code dependent} column with the given {@code degree}.
 *
 * <p>The {@code degree} is the fraction of rows for which the dependency holds: {@code 1.0}
 * is a hard dependency (the determinant tuple uniquely fixes the dependent value), {@code 0.0}
 * means no correlation (statistical independence). The CBO uses it to dampen the
 * independence assumption when estimating conjunctive-predicate selectivity.</p>
 *
 * @param determinants the determining columns (the "a" side); never empty
 * @param dependent    the determined column (the "b" side); must not appear in {@code determinants}
 * @param degree       dependency strength in [0.0, 1.0]
 */
public record FunctionalDependency(List<String> determinants, String dependent, double degree) {

    public FunctionalDependency {
        Objects.requireNonNull(determinants, "determinants must not be null");
        if (determinants.isEmpty()) {
            throw new IllegalArgumentException("determinants must not be empty");
        }
        if (dependent == null || dependent.isBlank()) {
            throw new IllegalArgumentException("dependent must not be null or blank");
        }
        if (determinants.contains(dependent)) {
            throw new IllegalArgumentException(
                    "dependent column '" + dependent + "' must not appear in determinants");
        }
        if (degree < 0.0 || degree > 1.0) {
            throw new IllegalArgumentException("degree must be in [0.0, 1.0], got " + degree);
        }
        determinants = List.copyOf(determinants);
    }
}
