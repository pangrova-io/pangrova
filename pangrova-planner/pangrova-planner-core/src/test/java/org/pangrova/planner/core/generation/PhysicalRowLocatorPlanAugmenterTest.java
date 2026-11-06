package org.pangrova.planner.core.generation;

import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.substrait.SubstraitProtocolVersion;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.protocol.capability.BuiltInCapabilityCatalog.PHYSICAL_ROW_POSITION_COLUMN;

class PhysicalRowLocatorPlanAugmenterTest {
    private static final List<String> SOURCE = List.of("catalog", "schema", "documents");

    @Test
    void keepsExpressionsAndExtensionsWhileShiftingComputedEmitOrdinals() throws Exception {
        Plan original = fixture();
        Plan result = augment(original, SOURCE);
        var root = result.getRelations(0).getRoot();
        var reorder = root.getInput().getProject();
        var project = reorder.getInput().getProject();
        var filter = project.getInput().getFilter();
        var read = filter.getInput().getRead();

        assertThat(root.getNamesList()).containsExactly(PHYSICAL_ROW_POSITION_COLUMN, "masked", "id");
        assertThat(reorder.getCommon().getEmit().getOutputMappingList()).containsExactly(2, 0, 1);
        assertThat(project.getExpressionsList())
                .isEqualTo(original.getRelations(0).getRoot().getInput().getProject().getExpressionsList());
        assertThat(project.getCommon().getEmit().getOutputMappingList()).containsExactly(3, 0, 2);
        assertThat(filter.getCondition()).isEqualTo(condition());
        assertThat(filter.getCommon().getEmit().getOutputMappingList()).containsExactly(0, 1, 2);
        assertThat(read.getBaseSchema().getNamesList()).containsExactly("id", "text", PHYSICAL_ROW_POSITION_COLUMN);
        assertThat(read.getBaseSchema().getStruct().getTypes(2).getI64().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(result.getVersion()).isEqualTo(original.getVersion());
        assertThat(result.getExtensionsList()).isEqualTo(original.getExtensionsList());
    }

    @Test
    void handlesRealCompilerProjectionAndPredicate() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("documents", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT)
                        .add("text", SqlTypeName.VARCHAR, 100).build();
            }
        });
        var relation = new PangrovaPlanner(schema).plan(
                "SELECT CASE WHEN id > 3 THEN 'masked' ELSE text END AS body "
                        + "FROM documents WHERE id > 1").plan();
        Plan original = Plan.parseFrom(new SubstraitPlanSerializer().serialize(
                new SubstraitConverter().convert(relation)));
        List<String> actualSource = read(original.getRelations(0).getRoot().getInput())
                .getNamedTable().getNamesList();
        Plan result = augment(original, actualSource);

        assertThat(result.getRelations(0).getRoot().getNamesList())
                .containsExactly(PHYSICAL_ROW_POSITION_COLUMN, "body");
        assertThat(result.getExtensionsList()).isEqualTo(original.getExtensionsList()).isNotEmpty();
        assertThat(read(result.getRelations(0).getRoot().getInput()).getBaseSchema().getNamesList())
                .contains(PHYSICAL_ROW_POSITION_COLUMN);
    }

    @Test
    void preservesReadEmitPruningAndDuplicateProjectionOutputs() throws Exception {
        Plan original = fixture();
        var project = original.getRelations(0).getRoot().getInput().getProject();
        var read = project.getInput().getFilter().getInput().getRead().toBuilder()
                .setCommon(emit(1, 1));
        var filter = project.getInput().getFilter().toBuilder().setInput(Rel.newBuilder().setRead(read));
        var duplicate = project.toBuilder().setInput(Rel.newBuilder().setFilter(filter)).setCommon(emit(2, 2));
        Plan changed = original.toBuilder().setRelations(0, original.getRelations(0).toBuilder()
                .setRoot(original.getRelations(0).getRoot().toBuilder()
                        .setInput(Rel.newBuilder().setProject(duplicate)))).build();

        Plan result = augment(changed, SOURCE);

        assertThat(read(result.getRelations(0).getRoot().getInput()).getCommon().getEmit().getOutputMappingList())
                .containsExactly(1, 1, 2);
        assertThat(result.getRelations(0).getRoot().getInput().getProject().getInput()
                .getProject().getCommon().getEmit().getOutputMappingList()).containsExactly(3, 3, 2);
    }

    @Test
    void rejectsWrongSourceForgedLocatorAndMalformedOutput() {
        Plan original = fixture();
        assertRejected(original, List.of("other"), "exact physical source");
        var root = original.getRelations(0).getRoot();
        assertRejected(original.toBuilder().setRelations(0, original.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setNames(0, PHYSICAL_ROW_POSITION_COLUMN))).build(),
                SOURCE, "output schema");
        assertRejected(original.toBuilder().setRelations(0, original.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().clearNames())).build(), SOURCE, "output schema");
        var invalid = root.getInput().getProject().toBuilder().setCommon(emit(100));
        assertRejected(original.toBuilder().setRelations(0, original.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setInput(Rel.newBuilder().setProject(invalid)))).build(),
                SOURCE, "out of bounds");
    }

    @Test
    void rejectsNonLinearPlansMultipleRootsAndInvalidBytes() {
        Plan original = fixture();
        var root = original.getRelations(0).getRoot();
        assertRejected(original.toBuilder().setRelations(0, original.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setInput(Rel.getDefaultInstance()))).build(), SOURCE, "linear");
        assertRejected(original.toBuilder().addRelations(original.getRelations(0)).build(), SOURCE, "single");
        assertThatThrownBy(() -> PhysicalRowLocatorPlanAugmenter.prepend(new byte[] {(byte) 0xff}, SOURCE))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("malformed");
    }

    private static Plan fixture() {
        Type integer = Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        var schema = NamedStruct.newBuilder().addNames("id").addNames("text")
                .setStruct(Type.Struct.newBuilder().addTypes(integer).addTypes(integer));
        var read = ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(SOURCE));
        var filter = FilterRel.newBuilder().setInput(Rel.newBuilder().setRead(read)).setCondition(condition());
        var project = ProjectRel.newBuilder().setInput(Rel.newBuilder().setFilter(filter))
                .addExpressions(field(1)).setCommon(emit(2, 0));
        return Plan.newBuilder().setVersion(SubstraitProtocolVersion.build())
                .addExtensions(io.substrait.proto.SimpleExtensionDeclaration.newBuilder().setExtensionFunction(
                        io.substrait.proto.SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                                .setFunctionAnchor(1).setName("is_not_null:i64")))
                .addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                        .setInput(Rel.newBuilder().setProject(project)).addNames("masked").addNames("id"))).build();
    }

    private static RelCommon emit(int... fields) {
        var emit = RelCommon.Emit.newBuilder();
        for (int field : fields) {
            emit.addOutputMapping(field);
        }
        return RelCommon.newBuilder().setEmit(emit).build();
    }

    private static Expression condition() {
        return Expression.newBuilder().setScalarFunction(Expression.ScalarFunction.newBuilder()
                .setFunctionReference(1).setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                .addArguments(io.substrait.proto.FunctionArgument.newBuilder().setValue(field(0)))).build();
    }

    private static Expression field(int index) {
        return Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
                .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance())
                .setDirectReference(Expression.ReferenceSegment.newBuilder()
                        .setStructField(Expression.ReferenceSegment.StructField.newBuilder().setField(index)))).build();
    }

    private static ReadRel read(Rel relation) {
        return switch (relation.getRelTypeCase()) {
            case READ -> relation.getRead();
            case FILTER -> read(relation.getFilter().getInput());
            case PROJECT -> read(relation.getProject().getInput());
            default -> throw new IllegalArgumentException("Unexpected relation");
        };
    }

    private static Plan augment(Plan original, List<String> source) throws Exception {
        return Plan.parseFrom(PhysicalRowLocatorPlanAugmenter.prepend(original.toByteArray(), source));
    }

    private static void assertRejected(Plan plan, List<String> source, String message) {
        assertThatThrownBy(() -> PhysicalRowLocatorPlanAugmenter.prepend(plan.toByteArray(), source))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message);
    }
}
