package org.pangrova.planner.cost;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pangrova.planner.stats.FunctionalDependency;
import org.pangrova.planner.stats.MultiColumnStatistics;
import org.junit.jupiter.api.Test;

class MultiColumnSelectivityEstimatorTest {

    private final MultiColumnSelectivityEstimator estimator = new MultiColumnSelectivityEstimator();

    private static Map<String, Double> sel(double city, double zip) {
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("city", city);
        m.put("zip", zip);
        return m;
    }

    @Test
    void independenceIsTheProduct() {
        assertThat(estimator.independent(sel(0.1, 0.2))).isEqualTo(0.02, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void noStatsFallsBackToIndependence() {
        double s = estimator.estimateConjunction(sel(0.1, 0.2), Optional.empty());
        assertThat(s).isEqualTo(0.02, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void perfectFunctionalDependencyMakesDependentPredicateFree() {
        // city ⇒ zip with degree 1.0: knowing city fully determines zip, so the conjunction
        // selectivity collapses to the determinant's selectivity (0.1), not the product (0.02).
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 0,
                List.of(new FunctionalDependency(List.of("city"), "zip", 1.0)));
        double s = estimator.estimateConjunction(sel(0.1, 0.2), Optional.of(stats));
        assertThat(s).isEqualTo(0.1, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void partialDependencyInterpolatesBetweenIndependenceAndDetermined() {
        // degree 0.5: zip contribution = 0.5 + 0.5*0.2 = 0.6; conjunction = 0.1 * 0.6 = 0.06.
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 0,
                List.of(new FunctionalDependency(List.of("city"), "zip", 0.5)));
        double s = estimator.estimateConjunction(sel(0.1, 0.2), Optional.of(stats));
        assertThat(s).isEqualTo(0.06, org.assertj.core.data.Offset.offset(1e-9));
        // strictly between independence (0.02) and full determination (0.1)
        assertThat(s).isGreaterThan(0.02).isLessThan(0.1);
    }

    @Test
    void zeroDegreeDependencyEqualsIndependence() {
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 0,
                List.of(new FunctionalDependency(List.of("city"), "zip", 0.0)));
        double s = estimator.estimateConjunction(sel(0.1, 0.2), Optional.of(stats));
        assertThat(s).isEqualTo(0.02, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void ndistinctProvidesSelectivityFloorForCorrelatedColumns() {
        // Independence would give 0.0001 (0.01*0.01) but only 50 distinct (city,zip) combos exist,
        // so selectivity cannot be below 1/50 = 0.02.
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 50, List.of());
        double s = estimator.estimateConjunction(sel(0.01, 0.01), Optional.of(stats));
        assertThat(s).isEqualTo(0.02, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void correctedEstimateIsCloserToTruthThanIndependence() {
        // Truth: highly correlated columns. Independence drastically under-estimates.
        Map<String, Double> perCol = sel(0.05, 0.05);
        double independence = estimator.independent(perCol); // 0.0025
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 40,
                List.of(new FunctionalDependency(List.of("city"), "zip", 0.9)));
        double corrected = estimator.estimateConjunction(perCol, Optional.of(stats));
        assertThat(corrected).isGreaterThan(independence);
    }

    @Test
    void statsNotCoveringPredicateColumnsFallsBackToIndependence() {
        Map<String, Double> perCol = new LinkedHashMap<>();
        perCol.put("city", 0.1);
        perCol.put("state", 0.2); // not in the stats group
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 10, List.of());
        double s = estimator.estimateConjunction(perCol, Optional.of(stats));
        assertThat(s).isEqualTo(0.02, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void singleColumnConjunctionUsesIndependence() {
        Map<String, Double> perCol = new LinkedHashMap<>();
        perCol.put("city", 0.1);
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 10, List.of());
        double s = estimator.estimateConjunction(perCol, Optional.of(stats));
        assertThat(s).isEqualTo(0.1, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void resultIsClampedToOne() {
        MultiColumnStatistics stats = new MultiColumnStatistics(
                List.of("city", "zip"), 1000, 0,
                List.of(new FunctionalDependency(List.of("city"), "zip", 1.0)));
        double s = estimator.estimateConjunction(sel(1.0, 1.0), Optional.of(stats));
        assertThat(s).isEqualTo(1.0, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void multiColumnStatisticsRejectsSingleColumn() {
        assertThatThrownBy(() -> new MultiColumnStatistics(List.of("city"), 1, 1, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiColumnStatisticsRejectsDuplicateColumns() {
        assertThatThrownBy(() -> new MultiColumnStatistics(List.of("city", "city"), 1, 1, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiColumnStatisticsRejectsDependencyOutsideGroup() {
        assertThatThrownBy(() -> new MultiColumnStatistics(
                List.of("city", "zip"), 1, 1,
                List.of(new FunctionalDependency(List.of("city"), "state", 0.5))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void functionalDependencyRejectsDependentInDeterminants() {
        assertThatThrownBy(() -> new FunctionalDependency(List.of("a", "b"), "a", 0.5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void functionalDependencyRejectsDegreeOutOfRange() {
        assertThatThrownBy(() -> new FunctionalDependency(List.of("a"), "b", 1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
