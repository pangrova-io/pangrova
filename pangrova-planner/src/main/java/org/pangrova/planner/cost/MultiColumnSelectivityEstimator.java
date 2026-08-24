package org.pangrova.planner.cost;

import org.pangrova.planner.stats.FunctionalDependency;
import org.pangrova.planner.stats.MultiColumnStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Estimates the combined selectivity of a conjunction of single-column equality predicates,
 * correcting the naive independence assumption ({@code S = ∏ s_i}) using multi-column
 * statistics. Mirrors PostgreSQL extended-statistics behaviour:
 *
 * <ul>
 *   <li><b>Functional dependencies</b>: for a dependency {@code a ⇒ b} of degree {@code d},
 *       the dependent column's contribution is {@code d + (1-d)·s_b} instead of {@code s_b}.
 *       At {@code d=1} the dependent predicate is "free" (already implied); at {@code d=0} it
 *       collapses back to independence.</li>
 *   <li><b>N-distinct</b>: when the predicate columns are covered by a group with a known
 *       combined NDV, the conjunction cannot be more selective than one distinct combination,
 *       so {@code 1/combinedNdv} is used as a floor on the estimate.</li>
 * </ul>
 *
 * <p>When no multi-column statistics are supplied the estimator returns the independence
 * baseline, preserving current behaviour.</p>
 */
public final class MultiColumnSelectivityEstimator {

    /** Independence baseline: the product of the per-column selectivities, clamped to [0,1]. */
    public double independent(Map<String, Double> perColumnSelectivity) {
        Objects.requireNonNull(perColumnSelectivity, "perColumnSelectivity must not be null");
        double s = 1.0;
        for (double v : perColumnSelectivity.values()) {
            s *= v;
        }
        return clamp(s);
    }

    /**
     * Best-available conjunction selectivity for the given per-column equality selectivities,
     * optionally corrected by a multi-column statistic over (a superset of) those columns.
     *
     * @param perColumnSelectivity per-column equality selectivity, keyed by column name
     * @param stats                optional multi-column statistic; correction is only applied
     *                             when it {@link MultiColumnStatistics#covers covers} all columns
     * @return the corrected selectivity in [0, 1]
     */
    public double estimateConjunction(Map<String, Double> perColumnSelectivity,
                                      Optional<MultiColumnStatistics> stats) {
        Objects.requireNonNull(perColumnSelectivity, "perColumnSelectivity must not be null");
        Objects.requireNonNull(stats, "stats must not be null (use Optional.empty())");
        double baseline = independent(perColumnSelectivity);
        if (perColumnSelectivity.size() < 2 || stats.isEmpty()
                || !stats.get().covers(perColumnSelectivity.keySet())) {
            return baseline;
        }
        MultiColumnStatistics mc = stats.get();
        double estimate = applyDependencies(perColumnSelectivity, mc.dependencies());
        if (mc.hasNdistinct()) {
            estimate = Math.max(estimate, 1.0 / mc.combinedNdv());
        }
        // Correlation can only raise selectivity above the independence product, never below it.
        return clamp(Math.max(estimate, baseline));
    }

    /**
     * Applies functional dependencies PostgreSQL-style: columns are processed in a stable order;
     * each column whose value is (partly) determined by already-processed columns contributes
     * {@code degree + (1-degree)·s} rather than its raw selectivity {@code s}.
     */
    private double applyDependencies(Map<String, Double> perColumnSelectivity,
                                     List<FunctionalDependency> dependencies) {
        List<String> columns = new ArrayList<>(perColumnSelectivity.keySet());
        Collections.sort(columns);
        Set<String> processed = new HashSet<>();
        double result = 1.0;
        for (String col : columns) {
            double s = perColumnSelectivity.get(col);
            double bestDegree = 0.0;
            for (FunctionalDependency d : dependencies) {
                if (d.dependent().equals(col) && processed.containsAll(d.determinants())) {
                    bestDegree = Math.max(bestDegree, d.degree());
                }
            }
            result *= bestDegree + (1.0 - bestDegree) * s;
            processed.add(col);
        }
        return result;
    }

    private static double clamp(double v) {
        return Math.max(0.0, Math.min(1.0, v));
    }
}
