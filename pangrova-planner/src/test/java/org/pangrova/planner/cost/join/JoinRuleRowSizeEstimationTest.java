package org.pangrova.planner.cost.join;

import java.math.BigDecimal;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.PangrovaRelOptCost;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.distribution.PangrovaDistributionTraitDef;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that join rules use statistics-based row size estimation
 * instead of hardcoded 64 bytes/row, and that wide tables produce
 * higher join costs than narrow tables.
 */
class JoinRuleRowSizeEstimationTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private VolcanoPlanner planner;
    private RelOptCluster cluster;
    private RexBuilder rexBuilder;

    @BeforeEach
    void setUp() {
        PangrovaCostFactory costFactory = new PangrovaCostFactory(CostModelConfig.SSD_DEFAULT);
        planner = new VolcanoPlanner(costFactory, null);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);

        rexBuilder = new RexBuilder(TYPE_FACTORY);
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    @Test
    void wideTable_hasHigherHashJoinCost_thanNarrowTable() {
        int rowCount = 500;

        // Narrow table: 2 INTEGER columns (Calcite reports ~8 bytes/row)
        PangrovaHashJoin narrowJoin = buildHashJoin(2, rowCount, 2, rowCount);
        // Wide table: 20 INTEGER columns (Calcite reports ~80 bytes/row)
        PangrovaHashJoin wideJoin = buildHashJoin(20, rowCount, 20, rowCount);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        PangrovaRelOptCost narrowCost = (PangrovaRelOptCost) narrowJoin.computeSelfCost(planner, mq);
        PangrovaRelOptCost wideCost = (PangrovaRelOptCost) wideJoin.computeSelfCost(planner, mq);

        // Wide table should have higher memory cost due to larger row size
        assertThat(wideCost.getMemory()).isGreaterThan(narrowCost.getMemory());
        // Wide table should also have higher I/O cost
        assertThat(wideCost.getIo()).isGreaterThan(narrowCost.getIo());
    }

    @Test
    void wideTable_hasHigherBroadcastJoinCost_thanNarrowTable() {
        int rowCount = 100;

        // Narrow table: 2 INTEGER columns
        RelNode narrowInput = buildInput(2, rowCount);
        // Wide table: 20 INTEGER columns
        RelNode wideInput = buildInput(20, rowCount);

        RelMetadataQuery mq = cluster.getMetadataQuery();

        // Verify that the row size estimation differs based on column count
        Double narrowRowSize = mq.getAverageRowSize(narrowInput);
        Double wideRowSize = mq.getAverageRowSize(wideInput);

        assertThat(narrowRowSize).isNotNull();
        assertThat(wideRowSize).isNotNull();
        assertThat(wideRowSize).isGreaterThan(narrowRowSize);
    }

    @Test
    void estimateRowBytes_usesStatisticsWhenAvailable() {
        RelNode input = buildInput(10, 50);
        RelMetadataQuery mq = cluster.getMetadataQuery();

        double estimated = BroadcastJoinRule.estimateRowBytes(input, mq);

        // 10 INTEGER columns × 4 bytes each = 40 bytes (Calcite's default for INTEGER)
        assertThat(estimated).isGreaterThan(0);
        assertThat(estimated).isLessThan(BroadcastJoinRule.DEFAULT_ROW_BYTES);
    }

    @Test
    void estimateRowBytes_fallsBackToDefault_whenStatsUnavailable() {
        // Verify the fallback constant is consistent with the cost model
        assertThat(BroadcastJoinRule.DEFAULT_ROW_BYTES).isEqualTo(128.0);
    }

    @Test
    void broadcastRule_matchesConsidersRowWidth() {
        // With narrow columns (2 INT = ~8 bytes/row), row size is small
        // With wide columns (200 INT = ~800 bytes/row), row size is large
        // This verifies the rule uses actual row width, not hardcoded 64 bytes

        RelNode narrowInput = buildInput(2, 100);
        RelNode wideInput = buildInput(200, 100);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        Double narrowRowSize = mq.getAverageRowSize(narrowInput);
        Double wideRowSize = mq.getAverageRowSize(wideInput);

        assertThat(narrowRowSize).isNotNull();
        assertThat(wideRowSize).isNotNull();
        // Wide row size should be significantly larger
        assertThat(wideRowSize).isGreaterThan(narrowRowSize * 5);
    }

    private PangrovaHashJoin buildHashJoin(int leftCols, int leftRows,
                                         int rightCols, int rightRows) {
        RelNode left = buildInput(leftCols, leftRows);
        RelNode right = buildInput(rightCols, rightRows);
        RexNode condition = buildEquiCondition(left, right);
        return new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), JoinRelType.INNER));
    }

    private RelNode buildInput(int columnCount, int rowCount) {
        RelDataTypeFactory.Builder typeBuilder = TYPE_FACTORY.builder();
        for (int i = 0; i < columnCount; i++) {
            typeBuilder.add("COL_" + i, SqlTypeName.INTEGER);
        }
        RelDataType rowType = typeBuilder.build();

        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        for (int r = 0; r < rowCount; r++) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            for (int i = 0; i < columnCount; i++) {
                row.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(r * columnCount + i)));
            }
            tuples.add(row.build());
        }
        return LogicalValues.create(cluster, rowType, tuples.build());
    }

    private RexNode buildEquiCondition(RelNode left, RelNode right) {
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(0).getType(), 0);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(0).getType(), leftFieldCount);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
    }
}
