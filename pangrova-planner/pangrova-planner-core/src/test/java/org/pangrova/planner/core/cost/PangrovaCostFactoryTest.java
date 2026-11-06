package org.pangrova.planner.core.cost;

import org.apache.calcite.plan.RelOptCost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the five-dimensional cost model populates memory and network
 * dimensions correctly, and that spill penalty influences plan comparison.
 */
class PangrovaCostFactoryTest {

    private PangrovaCostFactory factory;
    private CostModelConfig config;

    @BeforeEach
    void setUp() {
        config = CostModelConfig.SSD_DEFAULT;
        factory = new PangrovaCostFactory(config);
    }

    // --- Memory dimension ---

    @Test
    void makeCost_estimatesMemoryFromRowCount() {
        RelOptCost cost = factory.makeCost(1000, 50, 10);
        PangrovaRelOptCost pangrovaCost = (PangrovaRelOptCost) cost;

        double expectedMemory = 1000 * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW;
        assertThat(pangrovaCost.getMemory()).isEqualTo(expectedMemory);
    }

    @Test
    void makeCost_zeroRows_zeroMemory() {
        RelOptCost cost = factory.makeCost(0, 0, 0);
        PangrovaRelOptCost pangrovaCost = (PangrovaRelOptCost) cost;

        assertThat(pangrovaCost.getMemory()).isEqualTo(0.0);
    }

    @Test
    void logicalJoinRowsRemainCostlyWhenCalciteReportsZeroCpu() {
        RelOptCost small = factory.makeCost(10, 0, 0);
        RelOptCost large = factory.makeCost(1_000_000, 0, 0);

        assertThat(small.isLt(large)).isTrue();
        assertThat(large.isLt(small)).isFalse();
    }

    // --- Network dimension ---

    @Test
    void makeCostWithShuffle_estimatesNetworkCost() {
        double rows = 10_000;
        double memory = rows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW;
        PangrovaRelOptCost cost = factory.makeCostWithShuffle(rows, 100, 50, memory);

        double expectedNetwork = rows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW
                * config.networkCostPerByte();
        assertThat(cost.getNetwork()).isEqualTo(expectedNetwork);
        assertThat(cost.getNetwork()).isGreaterThan(0.0);
    }

    @Test
    void makeCostWithBroadcast_scalesByFanout() {
        double rows = 5_000;
        double memory = rows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW;
        int fanout = 8;
        PangrovaRelOptCost cost = factory.makeCostWithBroadcast(rows, 100, 50, memory, fanout);

        double expectedNetwork = rows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW
                * config.networkCostPerByte() * fanout;
        assertThat(cost.getNetwork()).isEqualTo(expectedNetwork);
    }

    @Test
    void broadcastCost_higherThanShuffleCost_forSameRows() {
        double rows = 10_000;
        double memory = rows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW;
        PangrovaRelOptCost shuffleCost = factory.makeCostWithShuffle(rows, 100, 50, memory);
        PangrovaRelOptCost broadcastCost = factory.makeCostWithBroadcast(rows, 100, 50, memory, 4);

        assertThat(broadcastCost.getNetwork()).isGreaterThan(shuffleCost.getNetwork());
    }

    // --- Spill penalty ---

    @Test
    void spillPenalty_triggersWhenMemoryExceedsBudget() {
        long budget = config.availableMemoryPerTask();
        double rowsWithinBudget = budget / PangrovaCostFactory.DEFAULT_BYTES_PER_ROW - 1;
        double rowsExceedingBudget = (budget / PangrovaCostFactory.DEFAULT_BYTES_PER_ROW) + 1000;

        RelOptCost withinBudget = factory.makeCost(rowsWithinBudget, 100, 50);
        RelOptCost exceedsBudget = factory.makeCost(rowsExceedingBudget, 100, 50);

        // The exceeding-budget cost should be significantly higher due to spill penalty
        assertThat(exceedsBudget.isLt(withinBudget)).isFalse();
    }

    @Test
    void spillPenalty_multipliesEffectiveCost() {
        // Create a config with small memory budget to easily trigger spill
        CostModelConfig smallBudget = new CostModelConfig(
                0.01, 0.005, 0.000122, 0.000150, 0.000300,
                0.03, 0.015, 0.04, 10.0, 1024);
        PangrovaCostFactory smallFactory = new PangrovaCostFactory(smallBudget);

        // 100 rows × 128 bytes = 12800 bytes >> 1024 budget → triggers spill
        PangrovaRelOptCost spillCost = (PangrovaRelOptCost) smallFactory.makeCost(100, 100, 50);
        // 1 row × 128 bytes = 128 bytes < 1024 budget → no spill
        PangrovaRelOptCost noSpillCost = (PangrovaRelOptCost) smallFactory.makeCost(1, 100, 50);

        // spillCost has same cpu+io base (100+50=150) but 10x penalty
        // noSpillCost has same cpu+io base (100+50=150) with 1x penalty
        // So spillCost effective = 150 * 10 = 1500, noSpillCost effective = 150 * 1 = 150
        assertThat(spillCost.isLt(noSpillCost)).isFalse();
        assertThat(noSpillCost.isLt(spillCost)).isTrue();
    }

    // --- Plan comparison: hash join vs scan-only ---

    @Test
    void largeHashJoinPlan_higherCostThanScanOnlyPlan() {
        // Scan-only plan: 10M rows, moderate CPU and I/O
        double scanRows = 10_000_000;
        double scanCpu = scanRows * config.cpuTupleCost();
        double scanIo = scanRows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW
                * config.seqIoCostPerByte();
        RelOptCost scanCost = factory.makeCost(scanRows, scanCpu, scanIo);

        // Hash join plan: build side 5M rows (large hash table) + probe 10M rows
        // with shuffle network cost on both sides
        double buildRows = 5_000_000;
        double probeRows = 10_000_000;
        double joinCpu = buildRows * config.hashBuildCostPerRow()
                + probeRows * config.hashProbeCostPerRow();
        double joinIo = (buildRows + probeRows) * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW
                * config.seqIoCostPerByte();
        double joinMemory = buildRows * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW;
        double joinNetwork = (buildRows + probeRows) * PangrovaCostFactory.DEFAULT_BYTES_PER_ROW
                * config.networkCostPerByte();
        PangrovaRelOptCost joinCost = factory.makeFiveDimCost(
                probeRows, joinCpu, joinIo, joinMemory, joinNetwork);

        // Hash join with 5M build rows (640MB) exceeds 256MB budget → spill penalty
        assertThat(joinCost.getMemory()).isGreaterThan(config.availableMemoryPerTask());
        assertThat(joinCost.isLt(scanCost)).isFalse();
        assertThat(scanCost.isLt(joinCost)).isTrue();
    }

    // --- Basic factory methods ---

    @Test
    void makeZeroCost_allDimensionsZero() {
        PangrovaRelOptCost zero = (PangrovaRelOptCost) factory.makeZeroCost();
        assertThat(zero.getRows()).isEqualTo(0.0);
        assertThat(zero.getCpu()).isEqualTo(0.0);
        assertThat(zero.getIo()).isEqualTo(0.0);
        assertThat(zero.getMemory()).isEqualTo(0.0);
        assertThat(zero.getNetwork()).isEqualTo(0.0);
    }

    @Test
    void makeInfiniteCost_isInfinite() {
        RelOptCost infinite = factory.makeInfiniteCost();
        assertThat(infinite.isInfinite()).isTrue();
    }

    @Test
    void makeFiveDimCost_preservesAllDimensions() {
        PangrovaRelOptCost cost = factory.makeFiveDimCost(100, 50, 30, 2000, 500);
        assertThat(cost.getRows()).isEqualTo(100.0);
        assertThat(cost.getIo()).isEqualTo(30.0);
        assertThat(cost.getMemory()).isEqualTo(2000.0);
        assertThat(cost.getNetwork()).isEqualTo(500.0);
    }
}
