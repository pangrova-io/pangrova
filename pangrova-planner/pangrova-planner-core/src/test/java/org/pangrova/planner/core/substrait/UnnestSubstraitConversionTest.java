package org.pangrova.planner.core.substrait;

import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Expression;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import io.substrait.relation.RelProtoConverter;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.TableFunctionDetail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual Calcite to generated-protocol oracles; Native values need separate Task tests. */
class UnnestSubstraitConversionTest {
    private final SqlTypeFactoryImpl types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private RelOptCluster cluster;
    private RexBuilder rex;
    private PlanNodeConverter converter;

    @BeforeEach
    void setUp() {
        var planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        rex = new RexBuilder(types);
        cluster = RelOptCluster.create(planner, rex);
        converter = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(), rex);
    }

    @Test
    void orderedDuplicateArgumentsBecomeSequentialSingleCollectionUnnest() throws Exception {
        var left = left(List.of(array(1, 2), array(3, 4, 5)));
        var correlation = new CorrelationId(0);
        var variable = rex.makeCorrel(left.getRowType(), correlation);
        var arguments = List.of(rex.makeFieldAccess(variable, 1), rex.makeFieldAccess(variable, 0),
                rex.makeFieldAccess(variable, 1));
        var uncollect = uncollect(arguments, true, List.of());
        var correlate = LogicalCorrelate.create(left, uncollect, List.of(), correlation,
                ImmutableBitSet.of(0, 1), JoinRelType.INNER);
        Rel proto = convert(correlate);
        var expansions = expansions(proto);
        assertThat(expansions).hasSize(3);
        assertThat(expansions.stream().map(detail -> detail.getInvocation().getArguments(0)
                .getExpression().getSelection().getDirectReference().getStructField().getField()))
                .containsExactly(4, 3, 2);
        for (var detail : expansions) {
            assertThat(detail.getInvocation().getCapability()).isEqualTo(BuiltInCapabilityCatalog.unnestPlanRef());
            assertThat(detail.getInvocation().getArgumentsCount()).isOne();
            assertThat(detail.getInvocation().getArguments(0).getName()).isEqualTo("input_0");
            assertOption(detail, "outer", false);
            assertOption(detail, "with_ordinality", true);
        }
        assertThat(SubstraitRelationLayout.describe(proto).outputSchema().getStruct().getTypesCount())
                .isEqualTo(correlate.getRowType().getFieldCount());
        Expression ordinal = proto.getProject().getExpressions(5);
        assertThat(ordinal.hasCast()).isTrue();
        assertThat(ordinal.getCast().getType().hasI32()).isTrue();
        assertThat(ordinal.getCast().getInput().hasScalarFunction()).isTrue();
        assertThat(Rel.parseFrom(proto.toByteArray())).isEqualTo(proto);
    }

    @Test
    void leftOuterGuardsEntireProductAndNullsComputedRightProjection() throws Exception {
        var left = left(List.of(array(1, 2), rex.makeNullLiteral(array(3).getType())));
        var correlation = new CorrelationId(1);
        var variable = rex.makeCorrel(left.getRowType(), correlation);
        var uncollect = uncollect(List.of(rex.makeFieldAccess(variable, 0),
                rex.makeFieldAccess(variable, 1)), false, List.of());
        var projected = LogicalProject.create(uncollect, List.of(), List.of(
                integer(42),
                new RexInputRef(1, uncollect.getRowType().getFieldList().get(1).getType())),
                List.of("constant", "second"));
        var correlate = LogicalCorrelate.create(left, projected, List.of(), correlation,
                ImmutableBitSet.of(0, 1), JoinRelType.LEFT);
        Rel proto = convert(correlate);
        for (var detail : expansions(proto)) assertOption(detail, "outer", true);
        assertThat(proto.getProject().getExpressionsCount()).isEqualTo(4);
        for (int i = 2; i < 4; i++) {
            var value = proto.getProject().getExpressions(i);
            assertThat(value.hasIfThen()).isTrue();
            assertThat(value.getIfThen().getIfsCount()).isOne();
            assertThat(value.getIfThen().getElse().getLiteral().hasNull()).isTrue();
            assertThat(SubstraitRelationLayout.typeOf(value,
                    SubstraitRelationLayout.describe(proto).expressionSchema()).getI32().getNullability())
                    .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        }
        Rel guards = projects(proto).stream().filter(project -> project.getProject().getExpressionsCount() == 2
                && project.getProject().getExpressions(0).hasIfThen()).findFirst().orElseThrow();
        for (var expression : guards.getProject().getExpressionsList()) {
            assertThat(expression.getIfThen().getElse().getLiteral().getNull().hasList()).isTrue();
        }
    }

    @Test
    void arrayRowFlattensOnlyWithoutItemAliasesAndMapHasTwoOutputs() throws Exception {
        RelDataType rowType = types.builder().add("a", SqlTypeName.INTEGER)
                .add("b", SqlTypeName.INTEGER).build();
        RexNode row = rex.makeCall(rowType, SqlStdOperatorTable.ROW,
                List.of(integer(1), integer(2)));
        RexNode rows = rex.makeCall(types.createArrayType(rowType, -1),
                SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, List.of(row));
        RexNode map = rex.makeNullLiteral(types.createMapType(
                types.createSqlType(SqlTypeName.VARCHAR), types.createSqlType(SqlTypeName.INTEGER)));
        var flattened = uncollect(List.of(rows, map), false, List.of());
        Rel proto = convert(flattened);
        assertThat(expansions(proto)).hasSize(2);
        assertThat(proto.getProject().getExpressionsCount()).isEqualTo(4);
        for (int i = 0; i < 2; i++) {
            assertThat(proto.getProject().getExpressions(i).getCast().getInput().getSelection()
                    .getDirectReference().getStructField().getChild().hasStructField()).isTrue();
        }
        var whole = uncollect(List.of(rows), false, List.of("whole_row"));
        Rel wholeProto = convert(whole);
        assertThat(wholeProto.getProject().getExpressionsCount()).isOne();
        assertThat(wholeProto.getProject().getExpressions(0).getCast().getType().hasStruct()).isTrue();
        assertThat(SubstraitRelationLayout.describe(wholeProto).outputSchema().getNamesList())
                .containsExactly("proj_0", "field_0", "field_1");
    }

    @Test
    void standaloneUncollectUsesSameTypedPathAndExplicitlyDropsReplicatedCollections() throws Exception {
        var uncollect = uncollect(List.of(array(1, 2), array(7, 8, 9)), true, List.of());
        Rel proto = convert(uncollect);
        assertThat(expansions(proto)).hasSize(2);
        assertThat(proto.getProject().getExpressionsCount()).isEqualTo(3);
        assertThat(proto.getProject().getCommon().getEmit().getOutputMappingCount()).isEqualTo(3);
        assertThat(SubstraitRelationLayout.describe(proto).outputSchema().getStruct().getTypesCount())
                .isEqualTo(uncollect.getRowType().getFieldCount());
        var ordinal = proto.getProject().getExpressions(2).getCast();
        assertThat(ordinal.getType().hasI32()).isTrue();
        assertThat(ordinal.getInput().getScalarFunction().getArgumentsCount()).isEqualTo(2);
    }

    private Rel convert(RelNode node) {
        return new RelProtoConverter(new ExtensionCollector()).toProto(converter.visit(node));
    }

    private LogicalProject left(List<RexNode> expressions) {
        var names = new ArrayList<String>();
        for (int i = 0; i < expressions.size(); i++) names.add("sql_alias_" + i);
        return LogicalProject.create(LogicalValues.createOneRow(cluster), List.of(), expressions, names);
    }

    private Uncollect uncollect(List<RexNode> arguments, boolean ordinal, List<String> aliases) {
        var argumentNames = new ArrayList<String>();
        for (int i = 0; i < arguments.size(); i++) argumentNames.add("argument_" + i);
        var input = LogicalProject.create(LogicalValues.createOneRow(cluster), List.of(), arguments, argumentNames);
        return Uncollect.create(input.getTraitSet(), input, ordinal, aliases);
    }

    private RexNode array(int... values) {
        var elements = new ArrayList<RexNode>();
        for (int value : values) elements.add(integer(value));
        return rex.makeCall(types.createArrayType(types.createSqlType(SqlTypeName.INTEGER), -1),
                SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elements);
    }

    private RexNode integer(int value) {
        return rex.makeExactLiteral(BigDecimal.valueOf(value), types.createSqlType(SqlTypeName.INTEGER));
    }

    private static List<TableFunctionDetail> expansions(Rel root) throws Exception {
        var details = new ArrayList<TableFunctionDetail>();
        Rel rel = root;
        while (!rel.hasRead()) {
            if (rel.hasProject()) rel = rel.getProject().getInput();
            else {
                var envelope = rel.getExtensionSingle().getDetail().unpack(RelationDetailEnvelope.class);
                assertThat(envelope.getSchemaVersion()).isOne();
                assertThat(envelope.hasTableFunction()).isTrue();
                details.add(envelope.getTableFunction());
                rel = rel.getExtensionSingle().getInput();
            }
        }
        return List.copyOf(details);
    }

    private static List<Rel> projects(Rel root) {
        var result = new ArrayList<Rel>();
        Rel rel = root;
        while (!rel.hasRead()) {
            if (rel.hasProject()) { result.add(rel); rel = rel.getProject().getInput(); }
            else rel = rel.getExtensionSingle().getInput();
        }
        return result;
    }

    private static void assertOption(TableFunctionDetail detail, String name, boolean expected) {
        var value = detail.getInvocation().getOptionsList().stream()
                .filter(option -> option.getName().equals(name)).findFirst().orElseThrow().getValue();
        assertThat(value.hasBoolValue()).isTrue();
        assertThat(value.getBoolValue()).isEqualTo(expected);
        assertThat(value.getType().getCanonicalSql()).isEqualTo("BOOLEAN");
        assertThat(value.getType().getSubstraitType().getBool().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
    }
}
