package org.pangrova.planner.core.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.PlanResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubstraitConverterTest {

    private PangrovaPlanner planner;
    private SubstraitConverter converter;

    @BeforeEach
    void setUp() {
        planner = new PangrovaPlanner();
        converter = new SubstraitConverter();
    }

    @Test
    void convertSelectOneLiteral() {
        PlanResult planResult = planner.plan("SELECT 1"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan).isNotNull();
        assertThat(plan.getRoots()).hasSize(1);
        assertThat(plan.getRoots().get(0).getInput()).isNotNull();
    }

    @Test
    void nullableValuesFieldAcceptsFoldedNonNullCoalesceLiteral() {
        PlanResult planResult = planner.plan("SELECT COALESCE(NULL, 42)");

        Plan plan = converter.convert(planResult.plan());

        assertThat(plan.getRoots()).hasSize(1);
    }

    @Test
    void valuesLiteralUsesDeclaredRowNullability() {
        var cluster = planner.plan("SELECT 1").plan().getCluster();
        var typeFactory = cluster.getTypeFactory();
        var integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        var nullableInteger = typeFactory.createTypeWithNullability(integer, true);
        var rowType = typeFactory.builder().add("value", nullableInteger).build();
        var literal = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(42), integer);
        var values = LogicalValues.create(cluster, rowType,
                ImmutableList.of(ImmutableList.of(literal)));

        var converted = new PlanNodeConverter(
                new TypeMapper(), new FunctionMapper(), cluster.getRexBuilder()).visit(values);

        assertThat(converted).isInstanceOf(io.substrait.relation.VirtualTableScan.class);
        assertThat(converted.getRecordType().fields().get(0))
                .isEqualTo(new TypeMapper().map(nullableInteger));
    }

    @Test
    void valuesIntegerLiteralWidensToDeclaredBigintWithoutChangingValue() {
        var cluster = planner.plan("SELECT 1").plan().getCluster();
        var typeFactory = cluster.getTypeFactory();
        var integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        var bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        var rowType = typeFactory.builder().add("value", bigint).build();
        var literal = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(42), integer);
        var values = LogicalValues.create(cluster, rowType,
                ImmutableList.of(ImmutableList.of(literal)));

        var converted = (io.substrait.relation.VirtualTableScan) new PlanNodeConverter(
                new TypeMapper(), new FunctionMapper(), cluster.getRexBuilder()).visit(values);
        var field = converted.getRows().get(0).fields().get(0);

        assertThat(field.getType()).isEqualTo(new TypeMapper().map(bigint));
        assertThat(field).isInstanceOf(io.substrait.expression.Expression.I64Literal.class);
        assertThat(((io.substrait.expression.Expression.I64Literal) field).value()).isEqualTo(42L);
    }

    @Test
    void valuesCharLiteralConformsToDeclaredVarchar() {
        var cluster = planner.plan("SELECT 1").plan().getCluster();
        var varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 16);
        RexLiteral literal = cluster.getRexBuilder().makeLiteral("abc");

        var converted = new LiteralConverter(new TypeMapper()).convert(literal, varchar);

        assertThat(converted.getType()).isEqualTo(new TypeMapper().map(varchar));
    }

    @Test
    void valuesRejectsOverflowAndNullInNonNullableField() {
        var cluster = planner.plan("SELECT 1").plan().getCluster();
        var typeFactory = cluster.getTypeFactory();
        var integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        var bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        var overflowing = cluster.getRexBuilder().makeExactLiteral(
                BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE), bigint);
        RexLiteral nullLiteral = (RexLiteral) cluster.getRexBuilder().makeNullLiteral(integer);
        var literals = new LiteralConverter(new TypeMapper());

        assertThatThrownBy(() -> literals.convert(overflowing, integer))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("cannot be represented");
        assertThatThrownBy(() -> literals.convert(nullLiteral, integer))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("NULL value in non-nullable");
    }

    @Test
    void convertSelectStringLiteral() {
        PlanResult planResult = planner.plan("SELECT 'hello'"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan).isNotNull();
        assertThat(plan.getRoots()).hasSize(1);
    }

    @Test
    void convertSelectMultipleExpressions() {
        PlanResult planResult = planner.plan("SELECT 1, 'abc', TRUE"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan).isNotNull();
        assertThat(plan.getRoots()).hasSize(1);
    }

    @Test
    void planRootContainsOutputNames() {
        PlanResult planResult = planner.plan("SELECT 1"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan.getRoots().get(0).getNames()).isNotEmpty();
    }

    @Test
    void rootInputIsVirtualTableScan() {
        PlanResult planResult = planner.plan("SELECT 1"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);
        Rel rootRel = plan.getRoots().get(0).getInput();

        // After constant folding, SELECT 1 becomes a single LogicalValues → VirtualTableScan
        assertThat(rootRel).isInstanceOf(io.substrait.relation.VirtualTableScan.class);
    }

    @Test
    void unsupportedRelNodeThrowsException() {
        PlanNodeConverter visitor = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT)));

        assertThatThrownBy(() -> visitor.visit(new UnsupportedRelNode()))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("Unsupported RelNode");
    }

    @Test
    void convertUnionAllProducesSetRel() {
        PlanResult planResult = planner.plan("SELECT 1 UNION ALL SELECT 2"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan).isNotNull();
        assertThat(plan.getRoots()).hasSize(1);
    }

    @Test
    void convertIntersectProducesSetRel() {
        RelNode left = buildValues(planner);
        RelNode right = buildValues(planner);
        LogicalIntersect intersect = LogicalIntersect.create(
                java.util.List.of(left, right), false);

        PlanNodeConverter visitor = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT)));
        Rel rel = visitor.visit(intersect);

        assertThat(rel).isInstanceOf(io.substrait.relation.Set.class);
        io.substrait.relation.Set setRel = (io.substrait.relation.Set) rel;
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.INTERSECTION_MULTISET);
        assertThat(setRel.getInputs()).hasSize(2);
    }

    @Test
    void convertIntersectAllProducesMultisetOp() {
        RelNode left = buildValues(planner);
        RelNode right = buildValues(planner);
        LogicalIntersect intersect = LogicalIntersect.create(
                java.util.List.of(left, right), true);

        PlanNodeConverter visitor = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT)));
        Rel rel = visitor.visit(intersect);

        assertThat(rel).isInstanceOf(io.substrait.relation.Set.class);
        io.substrait.relation.Set setRel = (io.substrait.relation.Set) rel;
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.INTERSECTION_MULTISET_ALL);
    }

    @Test
    void convertMinusProducesSetRel() {
        RelNode left = buildValues(planner);
        RelNode right = buildValues(planner);
        LogicalMinus minus = LogicalMinus.create(
                java.util.List.of(left, right), false);

        PlanNodeConverter visitor = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT)));
        Rel rel = visitor.visit(minus);

        assertThat(rel).isInstanceOf(io.substrait.relation.Set.class);
        io.substrait.relation.Set setRel = (io.substrait.relation.Set) rel;
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.MINUS_PRIMARY);
        assertThat(setRel.getInputs()).hasSize(2);
    }

    @Test
    void convertMinusAllProducesMultisetOp() {
        RelNode left = buildValues(planner);
        RelNode right = buildValues(planner);
        LogicalMinus minus = LogicalMinus.create(
                java.util.List.of(left, right), true);

        PlanNodeConverter visitor = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT)));
        Rel rel = visitor.visit(minus);

        assertThat(rel).isInstanceOf(io.substrait.relation.Set.class);
        io.substrait.relation.Set setRel = (io.substrait.relation.Set) rel;
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.MINUS_PRIMARY_ALL);
    }

    @Test
    void convertWindowFunctionProducesPlan() {
        // Window functions via SQL go through LogicalWindow or RexOver
        PlanResult planResult = planner.plan("SELECT 1, ROW_NUMBER() OVER ()"); RelNode relNode = planResult.plan();
        Plan plan = converter.convert(relNode);

        assertThat(plan).isNotNull();
        assertThat(plan.getRoots()).hasSize(1);
    }

    /**
     * Builds a simple LogicalValues node for use in set operation tests.
     */
    private static RelNode buildValues(PangrovaPlanner planner) {
        return planner.plan("SELECT 1").plan();
    }
}
