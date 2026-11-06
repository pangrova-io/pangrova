package org.pangrova.planner.core.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.proto.Rel;
import io.substrait.proto.SetRel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.extension.ExtensionCollector;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Calcite values -> sole SDK emitter -> generated canonical set protocol. */
class SetOperationConverterTest {
    private RexBuilder rex;
    private RelOptCluster cluster;
    private LogicalValues first;
    private LogicalValues second;
    private LogicalValues third;
    private PlanNodeConverter converter;

    @BeforeEach
    void setUp() {
        var types = new JavaTypeFactoryImpl();
        rex = new RexBuilder(types);
        cluster = RelOptCluster.create(new VolcanoPlanner(), rex);
        var rowType = types.builder().add("duplicate_label", SqlTypeName.INTEGER).build();
        first = LogicalValues.create(cluster, rowType, ImmutableList.of(
                ImmutableList.of(rex.makeExactLiteral(java.math.BigDecimal.ONE, rowType.getFieldList().get(0).getType())),
                ImmutableList.of(rex.makeExactLiteral(java.math.BigDecimal.ONE, rowType.getFieldList().get(0).getType()))));
        second = LogicalValues.create(cluster, rowType, ImmutableList.of(ImmutableList.of(
                rex.makeExactLiteral(java.math.BigDecimal.valueOf(2), rowType.getFieldList().get(0).getType()))));
        third = LogicalValues.createEmpty(cluster, rowType);
        converter = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), rex);
    }

    @Test
    void unionDistinctEmitsExactCanonicalOperationWithoutCapabilityDependentRewrite() throws Exception {
        assertOperation(LogicalUnion.create(List.of(first, second, third), false), SetRel.SetOp.SET_OP_UNION_DISTINCT);
    }

    @Test
    void unionAllPreservesExactCanonicalOperationAndOriginalRows() throws Exception {
        assertOperation(LogicalUnion.create(List.of(first, second, third), true), SetRel.SetOp.SET_OP_UNION_ALL);
    }

    @Test
    void intersectDistinctRequiresAllSecondaryInputsNotAnySecondary() throws Exception {
        assertOperation(LogicalIntersect.create(List.of(first, second, third), false), SetRel.SetOp.SET_OP_INTERSECTION_MULTISET);
    }

    @Test
    void intersectAllUsesActualCountedContractNotMultisetDistinct() throws Exception {
        assertOperation(LogicalIntersect.create(List.of(first, second, third), true), SetRel.SetOp.SET_OP_INTERSECTION_MULTISET_ALL);
    }

    @Test
    void exceptDistinctEmitsPrimaryDifference() throws Exception {
        assertOperation(LogicalMinus.create(List.of(first, second, third), false), SetRel.SetOp.SET_OP_MINUS_PRIMARY);
    }

    @Test
    void exceptAllUsesSumOfSecondaryMultiplicities() throws Exception {
        assertOperation(LogicalMinus.create(List.of(first, second, third), true), SetRel.SetOp.SET_OP_MINUS_PRIMARY_ALL);
    }

    private void assertOperation(RelNode node, SetRel.SetOp operation) throws Exception {
        Rel proto = new RelProtoConverter(new ExtensionCollector()).toProto(converter.visit(node));
        assertThat(proto.hasSet()).isTrue();
        assertThat(proto.getSet().getOp()).isEqualTo(operation);
        assertThat(proto.getSet().getInputsCount()).isEqualTo(3);
        assertThat(proto.getSet().getInputs(0).getRead().getVirtualTable().getExpressionsCount()).isEqualTo(2);
        assertThat(proto.getSet().getInputs(0).getRead().getVirtualTable().getExpressions(0).getFields(0)
                .getLiteral().getI32()).isEqualTo(1);
        assertThat(proto.getSet().getInputs(1).getRead().getVirtualTable().getExpressions(0).getFields(0)
                .getLiteral().getI32()).isEqualTo(2);
        assertThat(proto.getSet().getInputs(2).getRead().getVirtualTable().getExpressionsCount()).isZero();
        assertThat(SubstraitRelationLayout.describe(proto).outputSchema().getNamesList()).containsExactly("duplicate_label");
        assertThat(Rel.parseFrom(proto.toByteArray())).isEqualTo(proto);
    }
}
