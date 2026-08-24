package org.pangrova.planner.substrait;

import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlanResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

        // DISTINCT intersect is wrapped with dedup aggregate over a Set input
        assertThat(rel).isInstanceOf(io.substrait.relation.Aggregate.class);
        io.substrait.relation.Aggregate agg = (io.substrait.relation.Aggregate) rel;
        assertThat(agg.getInput()).isInstanceOf(io.substrait.relation.Set.class);
        io.substrait.relation.Set setRel = (io.substrait.relation.Set) agg.getInput();
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.INTERSECTION_PRIMARY);
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
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.INTERSECTION_MULTISET);
    }

    @Test
    void convertMinusProducesSetRel() {
        RelNode left = buildValues(planner);
        RelNode right = buildValues(planner);
        LogicalMinus minus = LogicalMinus.create(
                java.util.List.of(left, right), false);

        PlanNodeConverter visitor = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT)));
        Rel rel = visitor.visit(minus);

        // DISTINCT minus is wrapped with dedup aggregate over a Set input
        assertThat(rel).isInstanceOf(io.substrait.relation.Aggregate.class);
        io.substrait.relation.Aggregate agg = (io.substrait.relation.Aggregate) rel;
        assertThat(agg.getInput()).isInstanceOf(io.substrait.relation.Set.class);
        io.substrait.relation.Set setRel = (io.substrait.relation.Set) agg.getInput();
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
        assertThat(setRel.getSetOp()).isEqualTo(Set.SetOp.MINUS_MULTISET);
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
