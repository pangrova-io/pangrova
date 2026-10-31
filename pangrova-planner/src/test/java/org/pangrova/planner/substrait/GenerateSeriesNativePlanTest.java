package org.pangrova.planner.substrait;

import io.substrait.proto.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.lineage.ColumnDerivation;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.pangrova.planner.validation.SubstraitPlanValidator;
import org.pangrova.planner.validation.ValidationStatus;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerateSeriesNativePlanTest {
    private final PangrovaPlanner planner = new PangrovaPlanner();
    private final SubstraitConverter converter = new SubstraitConverter();

    @Test
    void directThreeArgumentSeriesUsesNativeTableFunctionRelation() throws Exception {
        var result = planner.plan("SELECT n FROM GENERATE_SERIES(10, 1, -3) AS s(n)");
        assertThat(containsTableFunction(result.plan())).isTrue();
        var plan = converter.convert(result.plan(), result.preOptimizationColumnNames());
        assertThat(new SubstraitPlanValidator(NativeCapabilityRegistry.defaults())
                .validate(plan).status()).isEqualTo(ValidationStatus.FULLY_NATIVE);
        Rel relation = io.substrait.proto.Plan.parseFrom(
                new SubstraitPlanSerializer().serialize(plan))
                .getRelations(0).getRoot().getInput();
        while (!relation.hasExtensionLeaf()) {
            assertThat(relation.hasProject()).isTrue();
            relation = relation.getProject().getInput();
        }
        var source = relation.getExtensionLeaf();
        var detail = source.getDetail().unpack(RelationDetailEnvelope.class).getTableFunction();
        assertThat(detail.getInvocation().getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.generateSeriesPlanRef());
        assertThat(detail.getInvocation().getArgumentsList().stream()
                .map(arg -> arg.getExpression().getLiteral().getI64()))
                .containsExactly(10L, 1L, -3L);
        assertThat(detail.getOutputSchema().getNamesList()).containsExactly("val");
        assertThat(detail.getOutputSchema().getStruct().getTypes(0).hasI64()).isTrue();
    }

    @Test
    void wrappedTwoArgumentSeriesDoesNotMaterializeValues() {
        var result = planner.plan("SELECT * FROM TABLE(GENERATE_SERIES(1, 1000000000))");
        assertThat(containsTableFunction(result.plan())).isTrue();
        assertThat(converter.convert(result.plan()).getRoots()).hasSize(1);
    }

    @Test
    void generatedSeriesOutputHasNoPhysicalSourceColumn() {
        var query = planner.plan("SELECT * FROM TABLE(GENERATE_SERIES(1, 3))").toPlannedQuery();
        var columns = query.columnLineage();
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).sourceColumns()).isEmpty();
        assertThat(columns.get(0).expression())
                .isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
    }

    @Test
    void projectedSeriesOutputRemainsDerivedWithoutPhysicalSource() {
        var query = planner.plan("SELECT n + 1 AS next_n FROM GENERATE_SERIES(3, 1, -1) AS s(n)")
                .toPlannedQuery();
        var columns = query.columnLineage();
        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).outputColumn()).isEqualTo("next_n");
        assertThat(columns.get(0).sourceColumns()).isEmpty();
        assertThat(columns.get(0).expression())
                .isEqualTo(ColumnDerivation.DerivationType.TRANSFORM);
    }

    @Test
    void signedInt64BoundsRemainExactInPhysicalArguments() throws Exception {
        var result = planner.plan("SELECT * FROM GENERATE_SERIES("
                + "-9223372036854775808, 9223372036854775807, 9223372036854775807)");
        var proto = io.substrait.proto.Plan.parseFrom(
                new SubstraitPlanSerializer().serialize(converter.convert(result.plan())));
        Rel relation = proto.getRelations(0).getRoot().getInput();
        while (!relation.hasExtensionLeaf()) {
            assertThat(relation.hasProject()).isTrue();
            relation = relation.getProject().getInput();
        }
        var arguments = relation.getExtensionLeaf().getDetail()
                .unpack(RelationDetailEnvelope.class).getTableFunction()
                .getInvocation().getArgumentsList();
        assertThat(arguments.stream().map(arg -> arg.getExpression().getLiteral().getI64()))
                .containsExactly(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Test
    void zeroStepFailsBeforeDispatch() {
        assertThatThrownBy(() -> converter.convert(planner.plan(
                "SELECT * FROM GENERATE_SERIES(1, 10, 0)").plan()))
                .hasMessageContaining("step must not be zero");
    }

    private static boolean containsTableFunction(RelNode relation) {
        if (relation instanceof LogicalTableFunctionScan) return true;
        return relation.getInputs().stream().anyMatch(GenerateSeriesNativePlanTest::containsTableFunction);
    }
}
