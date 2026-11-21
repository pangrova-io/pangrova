package org.pangrova.planner.analysis;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.join.BroadcastJoinRule;
import org.pangrova.planner.cost.join.HashJoinRule;
import org.pangrova.planner.cost.join.SortMergeJoinRule;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Equality algorithms must not win on an unrelated size/cost advantage. */
class JoinAlgorithmEligibilityTest {

    static Stream<RelOptRule> equalityRules() {
        return Stream.of(new HashJoinRule(CostModelConfig.SSD_DEFAULT),
                new BroadcastJoinRule(), new SortMergeJoinRule());
    }

    @ParameterizedTest
    @MethodSource("equalityRules")
    void rejectsCrossThetaDisjunctiveAndOneSidedConditions(RelOptRule rule) {
        for (String kind : List.of("cross", "theta", "disjunction", "one-sided", "false")) {
            var planner = new HepPlanner(new HepProgramBuilder().addRuleInstance(rule).build());
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var rex = new RexBuilder(types);
            var cluster = RelOptCluster.create(planner, rex);
            RelDataType rowType = types.builder().add("id", SqlTypeName.BIGINT).build();
            RelNode left = LogicalValues.create(cluster, rowType, ImmutableList.of(
                    ImmutableList.of(rex.makeBigintLiteral(BigDecimal.ONE))));
            RelNode right = LogicalValues.create(cluster, rowType, ImmutableList.of(
                    ImmutableList.of(rex.makeBigintLiteral(BigDecimal.TEN))));
            RexNode condition = condition(kind, rex, rowType);
            var join = LogicalJoin.create(left, right, List.of(), condition, Set.of(), JoinRelType.INNER);
            planner.setRoot(join);

            RelNode result = planner.findBestExp();

            assertThat(result).as("%s must reject %s without equality keys", rule, kind)
                    .isInstanceOf(LogicalJoin.class);
            assertThat(((Join) result).getCondition()).isEqualTo(condition);
        }
    }

    @Test
    void cboCrossJoinCompilesEveryDistributedFragment() {
        SqlCompiler compiler = compiler();
        assertDistributedFragmentsSerialize(compiler, "SELECT aa.id AS a_id, bb.id AS b_id, cc.id AS c_id "
                + "FROM a aa, b bb, c cc WHERE bb.id = cc.id");
    }

    @Test
    void tpchQ9ConvertsEveryCboDistributedJoinToSubstrait() throws IOException {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var decimal = types.createSqlType(SqlTypeName.DECIMAL, 15, 2);
        schema.add("part", table(types.builder().add("p_partkey", SqlTypeName.BIGINT)
                .add("p_name", SqlTypeName.VARCHAR, 55).build()));
        schema.add("supplier", table(types.builder().add("s_suppkey", SqlTypeName.BIGINT)
                .add("s_nationkey", SqlTypeName.BIGINT).build()));
        schema.add("lineitem", table(types.builder().add("l_orderkey", SqlTypeName.BIGINT)
                .add("l_partkey", SqlTypeName.BIGINT).add("l_suppkey", SqlTypeName.BIGINT)
                .add("l_extendedprice", decimal).add("l_discount", decimal)
                .add("l_quantity", decimal).build()));
        schema.add("partsupp", table(types.builder().add("ps_partkey", SqlTypeName.BIGINT)
                .add("ps_suppkey", SqlTypeName.BIGINT).add("ps_supplycost", decimal).build()));
        schema.add("orders", table(types.builder().add("o_orderkey", SqlTypeName.BIGINT)
                .add("o_orderdate", SqlTypeName.DATE).build()));
        schema.add("nation", table(types.builder().add("n_nationkey", SqlTypeName.BIGINT)
                .add("n_name", SqlTypeName.VARCHAR, 25).build()));
        try (var query = getClass().getResourceAsStream("/tpch/q09.sql")) {
            assertThat(query).isNotNull();
            var planned = compiler(schema).compileToResult(
                    new String(query.readAllBytes(), StandardCharsets.UTF_8));
            assertPhysicalJoinContracts(planned.plan());
            var fragmented = PlanFragmenter.fragment(planned.plan(), 4);
            // Exercise the actual join converter and wire serializer. Runtime function
            // capabilities (including YEAR) are discovered by the real worker in E2E.
            var converter = new SubstraitConverter();
            var serializer = new SubstraitPlanSerializer();
            assertThat(fragmented.stages()).isNotEmpty().allSatisfy(stage -> {
                var fragment = stage.fragment();
                assertThat(serializer.serialize(converter.convert(fragment.relNode(), fragment.fieldNames())))
                        .isNotEmpty();
            });
        }
    }

    private static void assertDistributedFragmentsSerialize(SqlCompiler compiler, String sql) {
        var planned = compiler.compileToResult(sql);
        assertPhysicalJoinContracts(planned.plan());
        FragmentedPlan fragmented = PlanFragmenter.fragment(planned.plan(), 4);

        assertThat(fragmented.stages()).isNotEmpty().allSatisfy(stage ->
                assertThat(stage.fragment().toSerializedPlan(compiler, stage.fragment().fieldNames())
                        .planBytes()).isNotEmpty());
    }

    private static void assertPhysicalJoinContracts(RelNode node) {
        if (node instanceof org.pangrova.planner.cost.join.PangrovaBroadcastJoin join) {
            assertThat(join.getRight()).as("broadcast build must retain its exchange: %s", node.explain())
                    .isInstanceOf(org.pangrova.planner.cost.distribution.PangrovaExchange.class);
        }
        for (RelNode input : node.getInputs()) {
            assertPhysicalJoinContracts(input);
        }
    }

    private static RexNode condition(String kind, RexBuilder rex, RelDataType rowType) {
        var type = rowType.getFieldList().get(0).getType();
        var left = rex.makeInputRef(type, 0);
        var right = rex.makeInputRef(type, 1);
        var equal = rex.makeCall(SqlStdOperatorTable.EQUALS, left, right);
        return switch (kind) {
            case "cross" -> rex.makeLiteral(true);
            case "theta" -> rex.makeCall(SqlStdOperatorTable.LESS_THAN, left, right);
            case "disjunction" -> rex.makeCall(SqlStdOperatorTable.OR, equal,
                    rex.makeCall(SqlStdOperatorTable.LESS_THAN, left, right));
            case "one-sided" -> rex.makeCall(SqlStdOperatorTable.EQUALS, left,
                    rex.makeBigintLiteral(BigDecimal.ONE));
            case "false" -> rex.makeLiteral(false);
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private static SqlCompiler compiler() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        for (String name : List.of("a", "b", "c")) {
            schema.add(name, table(types.builder().add("id", SqlTypeName.BIGINT).build()));
        }
        return compiler(schema);
    }

    private static AbstractTable table(RelDataType rowType) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory types) {
                return types.copyType(rowType);
            }

            @Override
            public Statistic getStatistic() {
                return Statistics.of(100D, List.of());
            }
        };
    }

    private static SqlCompiler compiler(CalciteSchema schema) {
        var context = new PlannerContext(null, null, null, null, null, null,
                CboConfig.DEFAULT, CostModelConfig.SSD_DEFAULT);
        return new SqlCompiler(new PangrovaPlanner(schema, List.of(), context),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }
}
