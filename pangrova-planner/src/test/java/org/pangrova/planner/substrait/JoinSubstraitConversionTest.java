package org.pangrova.planner.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.plan.Plan;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.Rel;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.PangrovaNestedLoopJoin;
import org.pangrova.planner.cost.join.PangrovaSortMergeJoin;
import org.pangrova.planner.cost.join.BroadcastJoinConfig;
import org.pangrova.planner.cost.join.HashJoinConfig;
import org.pangrova.planner.cost.join.NestedLoopJoinConfig;
import org.pangrova.planner.cost.join.SortMergeJoinConfig;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that all physical join operators (HashJoin, SortMergeJoin,
 * NestedLoopJoin, BroadcastJoin) convert to correct Substrait JoinRel
 * fields including join type, keys, and strategy metadata.
 */
class JoinSubstraitConversionTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private PlanNodeConverter converter;

    @BeforeEach
    void setUp() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, new RexBuilder(TYPE_FACTORY));
        rexBuilder = cluster.getRexBuilder();
        converter = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), rexBuilder);
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void hashJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaHashJoin hashJoin = buildHashJoin(joinType);

        Rel result = converter.visit(hashJoin);

        assertThat(result).isInstanceOf(ExtensionMulti.class);
        ExtensionMulti ext = (ExtensionMulti) result;
        assertThat(ext.getDetail()).isInstanceOf(HashJoinRelDetail.class);
        HashJoinRelDetail detail = (HashJoinRelDetail) ext.getDetail();
        assertThat(detail.strategy()).isEqualTo("hash_join");
        assertThat(detail.joinType()).isEqualTo(expectedJoinTypeString(joinType));
        assertThat(detail.leftKeys()).containsExactly(0);
        assertThat(detail.rightKeys()).containsExactly(0);
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void broadcastJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaBroadcastJoin broadcastJoin = buildBroadcastJoin(joinType);

        Rel result = converter.visit(broadcastJoin);

        assertThat(result).isInstanceOf(ExtensionMulti.class);
        ExtensionMulti ext = (ExtensionMulti) result;
        assertThat(ext.getDetail()).isInstanceOf(HashJoinRelDetail.class);
        HashJoinRelDetail detail = (HashJoinRelDetail) ext.getDetail();
        assertThat(detail.strategy()).isEqualTo("broadcast_join");
        assertThat(detail.joinType()).isEqualTo(expectedJoinTypeString(joinType));
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void sortMergeJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaSortMergeJoin mergeJoin = buildSortMergeJoin(joinType);

        Rel result = converter.visit(mergeJoin);

        assertThat(result).isInstanceOf(ExtensionMulti.class);
        ExtensionMulti ext = (ExtensionMulti) result;
        assertThat(ext.getDetail()).isInstanceOf(MergeJoinRelDetail.class);
        MergeJoinRelDetail detail = (MergeJoinRelDetail) ext.getDetail();
        assertThat(detail.joinType()).isEqualTo(expectedJoinTypeString(joinType));
        assertThat(detail.leftKeys()).containsExactly(0);
        assertThat(detail.rightKeys()).containsExactly(0);
        assertThat(detail.inputsPreSorted()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(JoinRelType.class)
    void nestedLoopJoinConvertsAllJoinTypes(JoinRelType joinType) {
        PangrovaNestedLoopJoin nestedLoopJoin = buildNestedLoopJoin(joinType);

        Rel result = converter.visit(nestedLoopJoin);

        assertThat(result).isInstanceOf(ExtensionMulti.class);
        ExtensionMulti ext = (ExtensionMulti) result;
        assertThat(ext.getDetail()).isInstanceOf(NestedLoopJoinRelDetail.class);
        NestedLoopJoinRelDetail detail = (NestedLoopJoinRelDetail) ext.getDetail();
        assertThat(detail.joinType()).isEqualTo(expectedJoinTypeString(joinType));
    }

    @Test
    void hashJoinFullPlanConversionProducesValidPlan() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.INNER);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(hashJoin);

        assertThat(plan.getRoots()).hasSize(1);
        assertThat(plan.getRoots().get(0).getInput()).isNotNull();
        assertThat(plan.getRoots().get(0).getNames()).isNotEmpty();
    }

    @Test
    void nestedLoopJoinFullPlanConversionProducesValidPlan() {
        PangrovaNestedLoopJoin nestedLoopJoin = buildNestedLoopJoin(JoinRelType.INNER);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(nestedLoopJoin);

        assertThat(plan.getRoots()).hasSize(1);
        assertThat(plan.getRoots().get(0).getInput()).isNotNull();
        assertThat(plan.getRoots().get(0).getNames()).isNotEmpty();
    }

    @Test
    void hashJoinSemiTypeProducesCorrectDetail() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.SEMI);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(hashJoin);

        Rel rootInput = plan.getRoots().get(0).getInput();
        assertThat(rootInput).isInstanceOf(ExtensionMulti.class);
        HashJoinRelDetail detail = (HashJoinRelDetail) ((ExtensionMulti) rootInput).getDetail();
        assertThat(detail.joinType()).isEqualTo("SEMI");
        assertThat(detail.strategy()).isEqualTo("hash_join");
    }

    @Test
    void hashJoinAntiTypeProducesCorrectDetail() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.ANTI);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        Plan plan = substraitConverter.convert(hashJoin);

        Rel rootInput = plan.getRoots().get(0).getInput();
        assertThat(rootInput).isInstanceOf(ExtensionMulti.class);
        HashJoinRelDetail detail = (HashJoinRelDetail) ((ExtensionMulti) rootInput).getDetail();
        assertThat(detail.joinType()).isEqualTo("ANTI");
    }

    @Test
    void hashJoinMultipleKeysProducesCorrectFieldReferences() {
        RelNode left = buildInput(4);
        RelNode right = buildInput(4);
        RexNode condition = buildMultiKeyCondition(left, right);
        PangrovaHashJoin hashJoin = new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), JoinRelType.INNER));

        Rel result = converter.visit(hashJoin);

        ExtensionMulti ext = (ExtensionMulti) result;
        HashJoinRelDetail detail = (HashJoinRelDetail) ext.getDetail();
        assertThat(detail.leftKeys()).containsExactly(0, 1);
        assertThat(detail.rightKeys()).containsExactly(0, 1);
    }

    private PangrovaHashJoin buildHashJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaHashJoin(new HashJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private PangrovaBroadcastJoin buildBroadcastJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaBroadcastJoin(new BroadcastJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private PangrovaSortMergeJoin buildSortMergeJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private PangrovaNestedLoopJoin buildNestedLoopJoin(JoinRelType joinType) {
        RelNode left = buildInput(3);
        RelNode right = buildInput(3);
        RexNode condition = buildEquiCondition(left, right, 0, 0);
        return new PangrovaNestedLoopJoin(new NestedLoopJoinConfig(
                cluster, left.getTraitSet(), left, right,
                condition, Set.of(), joinType));
    }

    private RelNode buildInput(int columnCount) {
        RelDataTypeFactory.Builder typeBuilder = TYPE_FACTORY.builder();
        for (int i = 0; i < columnCount; i++) {
            typeBuilder.add("COL_" + i, SqlTypeName.INTEGER);
        }
        RelDataType rowType = typeBuilder.build();
        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            row.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(i + 1)));
        }
        tuples.add(row.build());
        return LogicalValues.create(cluster, rowType, tuples.build());
    }

    private RexNode buildEquiCondition(RelNode left, RelNode right,
                                       int leftKeyIdx, int rightKeyIdx) {
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(leftKeyIdx).getType(), leftKeyIdx);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(rightKeyIdx).getType(),
                leftFieldCount + rightKeyIdx);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
    }

    private RexNode buildMultiKeyCondition(RelNode left, RelNode right) {
        RexNode cond1 = buildEquiCondition(left, right, 0, 0);
        RexNode cond2 = buildEquiCondition(left, right, 1, 1);
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);
    }

    private static String expectedJoinTypeString(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> "INNER";
            case LEFT -> "LEFT";
            case RIGHT -> "RIGHT";
            case FULL -> "OUTER";
            case SEMI -> "SEMI";
            case ANTI -> "ANTI";
        };
    }
}
