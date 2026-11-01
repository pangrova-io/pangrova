package org.pangrova.coordinator.indexing.filter;

import org.pangrova.spi.connector.index.predicate.EqualTo;
import org.pangrova.spi.connector.index.predicate.IndexPredicate;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AdaptivePruningCostModel} (G-INDEX-08).
 *
 * <p>The cost model gates L1 evaluation on three signals: L0 pruning ratio,
 * worst-coverage hint, and predicate selectivity. Each gate is verified
 * independently here so the wiring in {@link IndexSplitFilter} can rely on
 * a stable contract.</p>
 */
class AdaptivePruningCostModelTest {

    private final AdaptivePruningCostModel costModel = new AdaptivePruningCostModel();

    @Test
    void skipsWhenL0AlreadyPrunedAboveThreshold() {
        // L0 prune > 0.95 -> skip L1 even with full coverage and tight selectivity.
        assertThat(costModel.shouldSkipL1(0.96, 1.0, 0.01)).isTrue();
        assertThat(costModel.shouldSkipL1(0.95, 1.0, 0.01)).isFalse();
    }

    @Test
    void skipsWhenCoverageBelowThreshold() {
        // coverage < 0.50 -> skip L1 because we'd evaluate too few files.
        assertThat(costModel.shouldSkipL1(0.0, 0.49, 0.01)).isTrue();
        assertThat(costModel.shouldSkipL1(0.0, 0.50, 0.01)).isFalse();
    }

    @Test
    void skipsWhenSelectivityExceedsThreshold() {
        // selectivity > 0.50 -> skip L1 because the predicate matches too many rows.
        assertThat(costModel.shouldSkipL1(0.0, 1.0, 0.51)).isTrue();
        assertThat(costModel.shouldSkipL1(0.0, 1.0, 0.50)).isFalse();
    }

    @Test
    void allowsL1WhenNoSignalSkips() {
        // Healthy regime: L0 pruning low, full coverage, selective predicate.
        assertThat(costModel.shouldSkipL1(0.10, 1.0, 0.05)).isFalse();
    }

    @Test
    void selectivityFallsBackToHalfWhenStatsMissing() {
        IndexPredicate predicate = new EqualTo("col", 42);
        // Empty stats -> conservative 0.5 (neutral).
        assertThat(costModel.estimateSelectivity(List.of(), predicate)).isEqualTo(0.5);
        assertThat(costModel.estimateSelectivity(null, predicate)).isEqualTo(0.5);
    }

    @Test
    void selectivityUsesNdvWhenAvailable() {
        IndexPredicate predicate = new EqualTo("col", 42);
        // NDV=10 -> equality selectivity ~ 1/10 = 0.1.
        FileColumnStatistics stats = new FileColumnStatistics(
                "col", 0, 1000, 0L, 1000L, 10L, 4096L);
        double selectivity = costModel.estimateSelectivity(List.of(stats), predicate);
        assertThat(selectivity).isCloseTo(0.1, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void thresholdsAreEnforcedCorrectly() {
        // Verifies the boundary values of the internal thresholds via the public API.
        // L0 prune threshold is 0.95
        assertThat(costModel.shouldSkipL1(0.95, 1.0, 0.01)).isFalse();
        assertThat(costModel.shouldSkipL1(0.951, 1.0, 0.01)).isTrue();
        // Coverage threshold is 0.50
        assertThat(costModel.shouldSkipL1(0.0, 0.50, 0.01)).isFalse();
        assertThat(costModel.shouldSkipL1(0.0, 0.499, 0.01)).isTrue();
        // Selectivity threshold is 0.50
        assertThat(costModel.shouldSkipL1(0.0, 1.0, 0.50)).isFalse();
        assertThat(costModel.shouldSkipL1(0.0, 1.0, 0.501)).isTrue();
    }
}
