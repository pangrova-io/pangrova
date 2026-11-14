package org.pangrova.planner.cost.join;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.PangrovaRelOptCost;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.distribution.PangrovaDistributionTraitDef;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSort;
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
 * Verifies that SortMergeJoin cost estimation correctly detects pre-sorted
 * inputs and yields O(n+m) merge cost instead of O(n log n) sort + merge cost.
 * When both inputs are pre-sorted on join keys, the optimizer should prefer
 * SortMergeJoin over HashJoin.
 */
class SortMergeJoinCostEstimationTest {

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
    void preSortedInputs_yieldLowerCpuCost_thanUnsortedInputs() {
        int rowCount = 1000;

        PangrovaSortMergeJoin preSortedJoin = buildSortMergeJoin(
                3, rowCount, 3, rowCount, List.of(0), List.of(0), true);
        PangrovaSortMergeJoin unsortedJoin = buildSortMergeJoin(
                3, rowCount, 3, rowCount, List.of(0), List.of(0), false);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        PangrovaRelOptCost preSortedCost =
                (PangrovaRelOptCost) preSortedJoin.computeSelfCost(planner, mq);
        PangrovaRelOptCost unsortedCost =
                (PangrovaRelOptCost) unsortedJoin.computeSelfCost(planner, mq);

        // Pre-sorted: O(n+m) merge only; unsorted: O(n log n) sort + O(n+m) merge
        assertThat(preSortedCost.getCpu())
                .as("Pre-sorted CPU should be significantly lower (no sort cost)")
                .isLessThan(unsortedCost.getCpu());
    }

    @Test
    void preSortedInputs_yieldMinimalMemory() {
        int rowCount = 500;

        PangrovaSortMergeJoin preSortedJoin = buildSortMergeJoin(
                3, rowCount, 3, rowCount, List.of(0), List.of(0), true);
        PangrovaSortMergeJoin unsortedJoin = buildSortMergeJoin(
                3, rowCount, 3, rowCount, List.of(0), List.of(0), false);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        PangrovaRelOptCost preSortedCost =
                (PangrovaRelOptCost) preSortedJoin.computeSelfCost(planner, mq);
        PangrovaRelOptCost unsortedCost =
                (PangrovaRelOptCost) unsortedJoin.computeSelfCost(planner, mq);

        // Pre-sorted: streaming merge buffer only; unsorted: full sort buffers
        assertThat(preSortedCost.getMemory())
                .as("Pre-sorted memory should be minimal (streaming merge buffer)")
                .isLessThan(unsortedCost.getMemory());

        // Streaming buffer = 2 rows × (leftRowWidth + rightRowWidth)
        // For 3 INTEGER columns: rowWidth = 4 * 3 = 12, so memory = 2 * (12 + 12) = 48
        assertThat(preSortedCost.getMemory()).isGreaterThan(0.0);
        assertThat(preSortedCost.getMemory()).isLessThan(100.0);
    }

    @Test
    void preSortedSortMergeJoin_cheaperThanHashJoin_forSameInputs() {
        int rowCount = 1000;

        // Both inputs sorted on join key (column 0)
        PangrovaSortMergeJoin sortMergeJoin = buildSortMergeJoin(
                3, rowCount, 3, rowCount, List.of(0), List.of(0), true);

        // Hash join on same inputs (hash join doesn't benefit from sort order)
        RelNode left = buildSortedInput(3, rowCount, List.of(0));
        RelNode right = buildSortedInput(3, rowCount, List.of(0));
        RexNode condition = buildEquiCondition(left, right, 0);
        PangrovaHashJoin hashJoin = new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), JoinRelType.INNER));

        RelMetadataQuery mq = cluster.getMetadataQuery();
        PangrovaRelOptCost smjCost =
                (PangrovaRelOptCost) sortMergeJoin.computeSelfCost(planner, mq);
        PangrovaRelOptCost hjCost =
                (PangrovaRelOptCost) hashJoin.computeSelfCost(planner, mq);

        // When inputs are pre-sorted, SortMergeJoin should be cheaper overall
        // (lower CPU due to no sort, lower memory due to streaming)
        assertThat(smjCost.getCpu())
                .as("Pre-sorted SMJ CPU should be lower than hash join CPU")
                .isLessThan(hjCost.getCpu());
        assertThat(smjCost.getMemory())
                .as("Pre-sorted SMJ memory should be lower than hash join memory")
                .isLessThan(hjCost.getMemory());
    }

    @Test
    void onlyLeftSorted_yieldsFullSortCost() {
        int rowCount = 500;

        // Only left is sorted, right is not
        RelNode leftSorted = buildSortedInput(3, rowCount, List.of(0));
        RelNode rightUnsorted = buildUnsortedInput(3, rowCount);
        RexNode condition = buildEquiCondition(leftSorted, rightUnsorted, 0);

        PangrovaSortMergeJoin partialSortJoin = new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                cluster, leftSorted.getTraitSet(), leftSorted, rightUnsorted,
                condition, Set.of(), JoinRelType.INNER));

        // Both sorted
        PangrovaSortMergeJoin fullySortedJoin = buildSortMergeJoin(
                3, rowCount, 3, rowCount, List.of(0), List.of(0), true);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        PangrovaRelOptCost partialCost =
                (PangrovaRelOptCost) partialSortJoin.computeSelfCost(planner, mq);
        PangrovaRelOptCost fullySortedCost =
                (PangrovaRelOptCost) fullySortedJoin.computeSelfCost(planner, mq);

        // Partially sorted should have higher cost (needs to sort the unsorted side)
        assertThat(partialCost.getCpu()).isGreaterThan(fullySortedCost.getCpu());
        assertThat(partialCost.getMemory()).isGreaterThan(fullySortedCost.getMemory());
    }

    @Test
    void multiKeySort_detectedAsPreSorted() {
        int rowCount = 200;

        // Both inputs sorted on columns 0 and 1 (composite join key)
        PangrovaSortMergeJoin multiKeyJoin = buildSortMergeJoin(
                4, rowCount, 4, rowCount, List.of(0, 1), List.of(0, 1), true);

        // Same inputs but unsorted
        PangrovaSortMergeJoin unsortedJoin = buildSortMergeJoin(
                4, rowCount, 4, rowCount, List.of(0, 1), List.of(0, 1), false);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        PangrovaRelOptCost sortedCost =
                (PangrovaRelOptCost) multiKeyJoin.computeSelfCost(planner, mq);
        PangrovaRelOptCost unsortedCost =
                (PangrovaRelOptCost) unsortedJoin.computeSelfCost(planner, mq);

        assertThat(sortedCost.getCpu()).isLessThan(unsortedCost.getCpu());
        // Streaming buffer = 2 rows × (leftRowWidth + rightRowWidth), much less than full sort buffers
        assertThat(sortedCost.getMemory()).isGreaterThan(0.0);
        assertThat(sortedCost.getMemory()).isLessThan(unsortedCost.getMemory());
    }

    // ── Helper methods ──

    private PangrovaSortMergeJoin buildSortMergeJoin(
            int leftCols, int leftRows, int rightCols, int rightRows,
            List<Integer> leftKeys, List<Integer> rightKeys, boolean preSorted) {
        RelNode left = preSorted
                ? buildSortedInput(leftCols, leftRows, leftKeys)
                : buildUnsortedInput(leftCols, leftRows);
        RelNode right = preSorted
                ? buildSortedInput(rightCols, rightRows, rightKeys)
                : buildUnsortedInput(rightCols, rightRows);
        RexNode condition = buildEquiCondition(left, right, leftKeys, rightKeys);
        return new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), JoinRelType.INNER));
    }

    private RelNode buildSortedInput(int columnCount, int rowCount, List<Integer> sortKeys) {
        RelNode base = buildUnsortedInput(columnCount, rowCount);
        List<RelFieldCollation> fieldCollations = sortKeys.stream()
                .map(idx -> new RelFieldCollation(idx, RelFieldCollation.Direction.ASCENDING))
                .toList();
        RelCollation collation = RelCollations.of(fieldCollations);
        LogicalSort sort = LogicalSort.create(base, collation, null, null);
        RelTraitSet traitSetWithCollation = sort.getTraitSet().plus(collation);
        return sort.copy(traitSetWithCollation, sort.getInputs());
    }

    private RelNode buildUnsortedInput(int columnCount, int rowCount) {
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

    private RexNode buildEquiCondition(RelNode left, RelNode right, int joinKeyIndex) {
        return buildEquiCondition(left, right, List.of(joinKeyIndex), List.of(joinKeyIndex));
    }

    private RexNode buildEquiCondition(RelNode left, RelNode right,
                                       List<Integer> leftKeys, List<Integer> rightKeys) {
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode condition = null;

        for (int i = 0; i < leftKeys.size(); i++) {
            RexNode leftRef = rexBuilder.makeInputRef(
                    left.getRowType().getFieldList().get(leftKeys.get(i)).getType(),
                    leftKeys.get(i));
            RexNode rightRef = rexBuilder.makeInputRef(
                    right.getRowType().getFieldList().get(rightKeys.get(i)).getType(),
                    leftFieldCount + rightKeys.get(i));
            RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
            condition = (condition == null) ? eq
                    : rexBuilder.makeCall(SqlStdOperatorTable.AND, condition, eq);
        }
        return condition;
    }
}
