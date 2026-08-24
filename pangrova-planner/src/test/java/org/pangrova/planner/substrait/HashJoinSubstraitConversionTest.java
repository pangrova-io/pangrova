package org.pangrova.planner.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.plan.Plan;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.Rel;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.BroadcastJoinConfig;
import org.pangrova.planner.cost.join.HashJoinConfig;
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

import java.math.BigDecimal;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies that CBO-selected physical join operators (PangrovaHashJoin,
 * PangrovaBroadcastJoin) convert to Substrait without throwing.
 */
class HashJoinSubstraitConversionTest {

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

    @Test
    void hashJoinConvertsToExtensionMulti() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.INNER);

        Rel result = converter.visit(hashJoin);

        assertThat(result).isInstanceOf(ExtensionMulti.class);
        ExtensionMulti ext = (ExtensionMulti) result;
        assertThat(ext.getDetail()).isInstanceOf(HashJoinRelDetail.class);
        HashJoinRelDetail detail = (HashJoinRelDetail) ext.getDetail();
        assertThat(detail.strategy()).isEqualTo("hash_join");
        assertThat(detail.joinType()).isEqualTo("INNER");
        assertThat(detail.leftKeys()).containsExactly(0);
        assertThat(detail.rightKeys()).containsExactly(0);
    }

    @Test
    void broadcastJoinConvertsToExtensionMulti() {
        PangrovaBroadcastJoin broadcastJoin = buildBroadcastJoin(JoinRelType.LEFT);

        Rel result = converter.visit(broadcastJoin);

        assertThat(result).isInstanceOf(ExtensionMulti.class);
        ExtensionMulti ext = (ExtensionMulti) result;
        assertThat(ext.getDetail()).isInstanceOf(HashJoinRelDetail.class);
        HashJoinRelDetail detail = (HashJoinRelDetail) ext.getDetail();
        assertThat(detail.strategy()).isEqualTo("broadcast_join");
        assertThat(detail.joinType()).isEqualTo("LEFT");
    }

    @Test
    void hashJoinFullPlanConversionSucceeds() {
        PangrovaHashJoin hashJoin = buildHashJoin(JoinRelType.INNER);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        assertThatCode(() -> substraitConverter.convert(hashJoin))
                .doesNotThrowAnyException();

        Plan plan = substraitConverter.convert(hashJoin);
        assertThat(plan.getRoots()).hasSize(1);
        assertThat(plan.getRoots().get(0).getInput()).isNotNull();
    }

    @Test
    void broadcastJoinFullPlanConversionSucceeds() {
        PangrovaBroadcastJoin broadcastJoin = buildBroadcastJoin(JoinRelType.RIGHT);
        SubstraitConverter substraitConverter = new SubstraitConverter();

        assertThatCode(() -> substraitConverter.convert(broadcastJoin))
                .doesNotThrowAnyException();

        Plan plan = substraitConverter.convert(broadcastJoin);
        assertThat(plan.getRoots()).hasSize(1);
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
}
