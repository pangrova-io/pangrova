package org.pangrova.protocol.generation;

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
import io.substrait.proto.SetRel;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Serialized-plan contract tests; these do not substitute for the Native/public SQL execution gate. */
class GeneratedValuesOverlayPlanBinderTest {
    private static final List<String> TABLE = List.of("lake", "sales", "documents");

    @Test
    void restoresDuplicateReadEmitAfterFullSchemaFilterWithoutShiftingOrdinals() throws Exception {
        var condition = isNotNull(1);
        var read = read().toBuilder().setFilter(condition).setBestEffortFilter(condition)
                .setCommon(RelCommon.newBuilder().setEmit(RelCommon.Emit.newBuilder()
                        .addOutputMapping(2).addOutputMapping(0).addOutputMapping(2))).build();
        var bound = bind(plan(Rel.newBuilder().setRead(read).build()),
                List.of(intent(0, "overlay-0")));
        var root = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot();
        var emitted = root.getInput().getProject();
        assertThat(emitted.getCommon().getEmit().getOutputMappingList()).containsExactly(2, 0, 2);
        var filter = emitted.getInput().getFilter();
        assertThat(filter.getCondition()).isEqualTo(condition);
        var restored = filter.getInput().getProject();
        assertThat(restored.getCommon().getEmit().getOutputMappingList()).containsExactly(0, 1, 4);
        var overlay = restored.getInput().getExtensionSingle();
        var detail = overlay.getDetail().unpack(RelationDetailEnvelope.class).getTableFunction();
        assertThat(detail.getPlanNodeId()).isEqualTo("overlay-0");
        assertThat(detail.getOutputSchema().getNamesList()).containsExactly(
                "id", "content", "$pangrova$source_object_id", "$pangrova$row_position", "embedding");
        assertThat(detail.getInvocation().getArgumentsList())
                .extracting(argument -> argument.getExpression().getSelection()
                        .getDirectReference().getStructField().getField()).containsExactly(2, 3);
        var physical = overlay.getInput().getRead();
        assertThat(physical.hasFilter()).isFalse();
        assertThat(physical.hasBestEffortFilter()).isFalse();
        assertThat(physical.getCommon().getEmit().getOutputMappingList()).containsExactly(0, 1, 3, 4);
        assertThat(bound.operatorBindings()).containsExactly(intent(0, "overlay-0").binding());
    }

    @Test
    void overlaysBeforeExistingPolicyFilterAndMaskProjectWithoutChangingTheirExpressions() throws Exception {
        var condition = isNotNull(0);
        var mask = Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setString("masked")).build();
        var protectedRead = Rel.newBuilder().setFilter(FilterRel.newBuilder()
                .setInput(Rel.newBuilder().setRead(read())).setCondition(condition));
        var protectedPlan = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(protectedRead)
                .addExpressions(mask).setCommon(RelCommon.newBuilder().setEmit(RelCommon.Emit.newBuilder()
                        .addOutputMapping(0).addOutputMapping(3).addOutputMapping(2)))).build();
        var bound = bind(plan(protectedPlan), List.of(intent(0, "overlay")));
        var project = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput().getProject();
        assertThat(project.getExpressionsList()).containsExactly(mask);
        assertThat(project.getCommon()).isEqualTo(protectedPlan.getProject().getCommon());
        assertThat(project.getInput().getFilter().getCondition()).isEqualTo(condition);
        assertThat(project.getInput().getFilter().getInput().hasProject()).isTrue();
    }

    @Test
    void retainsNonEmittedFilterColumnAndCanReplaceGeneratedFilterOnlyColumn() throws Exception {
        var read = read().toBuilder().setFilter(isNotNull(2)).setCommon(RelCommon.newBuilder()
                .setEmit(RelCommon.Emit.newBuilder().addOutputMapping(0))).build();
        var bound = bind(plan(Rel.newBuilder().setRead(read).build()),
                List.of(intent(0, "filter-only")));
        var project = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput().getProject();
        assertThat(project.getCommon().getEmit().getOutputMappingList()).containsExactly(0);
        assertThat(project.getInput().getFilter().getCondition()).isEqualTo(isNotNull(2));
        assertThat(project.getInput().getFilter().getInput().getProject()
                .getCommon().getEmit().getOutputMappingList()).containsExactly(0, 1, 4);
    }

    @Test
    void bindsRepeatedReadsByExactOrdinalRatherThanTableNameAlone() throws Exception {
        var original = Plan.parseFrom(plan(Rel.newBuilder().setRead(read()).build()));
        var repeated = original.toBuilder().addRelations(original.getRelations(0)).build().toByteArray();
        var bound = bind(repeated,
                List.of(intent(0, "left"), intent(1, "right")));
        assertThat(bound.operatorBindings()).extracting(OperatorBinding::getPlanNodeId)
                .containsExactly("left", "right");
        var verified = GeneratedValuesOverlayPlanBinder.resolve(bound.planBytes(), reads -> {
            assertThat(reads).hasSize(2);
            return List.of();
        });
        assertThat(verified.planBytes()).containsExactly(bound.planBytes());
    }

    @Test
    void preservesRepeatedSetScanOrderAndLeavesOtherInputsIntact() throws Exception {
        var first = read().toBuilder().setFilter(isNotNull(0)).build();
        var second = read().toBuilder().setFilter(isNotNull(1)).build();
        var firstInput = Rel.newBuilder().setRead(first).build();
        var secondInput = Rel.newBuilder().setRead(second).build();
        var set = SetRel.newBuilder().setOp(SetRel.SetOp.SET_OP_UNION_ALL)
                .addInputs(firstInput).addInputs(secondInput).addInputs(firstInput);
        byte[] original = plan(Rel.newBuilder().setSet(set).build());
        var bound = GeneratedValuesOverlayPlanBinder.resolve(original, reads -> {
            assertThat(reads).containsExactly(first, second, first);
            return List.of(intent(1, "middle"));
        });
        var rewritten = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput().getSet();
        assertThat(rewritten.getOp()).isEqualTo(set.getOp());
        assertThat(rewritten.getInputs(0)).isEqualTo(firstInput);
        assertThat(rewritten.getInputs(1).getProject().getInput().getFilter().getCondition()).isEqualTo(second.getFilter());
        assertThat(rewritten.getInputs(2)).isEqualTo(firstInput);
        assertThat(bound.operatorBindings()).containsExactly(intent(1, "middle").binding());
    }

    @Test
    void rejectsWrongReadIdentityDuplicateTargetAndMissingReadOrdinal() {
        byte[] original = plan(Rel.newBuilder().setRead(read()).build());
        var wrong = new GeneratedValuesOverlayPlanBinder.ReadOverlay(
                0, List.of("lake", "sales", "other"), "embedding", binding("wrong"));
        assertThatThrownBy(() -> bind(original, List.of(wrong)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("complete named");
        assertThatThrownBy(() -> bind(original,
                List.of(intent(0, "first"), intent(0, "second"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicated");
        assertThatThrownBy(() -> bind(original, List.of(intent(1, "missing"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exactly one");
    }

    @Test
    void rejectsScalarTargetHiddenNameCollisionAndOutOfBoundsEmit() {
        var scalar = read().toBuilder().setBaseSchema(read().getBaseSchema().toBuilder()
                .setStruct(read().getBaseSchema().getStruct().toBuilder()
                        .setTypes(2, Type.newBuilder().setFp32(Type.FP32.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_NULLABLE))))).build();
        var hidden = read().toBuilder().setBaseSchema(read().getBaseSchema().toBuilder()
                .setNames(0, "$pangrova$row_position")).build();
        var badEmit = read().toBuilder().setCommon(RelCommon.newBuilder()
                .setEmit(RelCommon.Emit.newBuilder().addOutputMapping(3))).build();
        for (var invalid : List.of(scalar, hidden, badEmit)) {
            assertThatThrownBy(() -> bind(
                    plan(Rel.newBuilder().setRead(invalid).build()), List.of(intent(0, "invalid"))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static BoundExecutionPlan bind(byte[] bytes, List<GeneratedValuesOverlayPlanBinder.ReadOverlay> overlays) {
        return GeneratedValuesOverlayPlanBinder.resolve(bytes, reads -> overlays);
    }

    private static GeneratedValuesOverlayPlanBinder.ReadOverlay intent(int ordinal, String node) {
        return new GeneratedValuesOverlayPlanBinder.ReadOverlay(ordinal, TABLE, "embedding", binding(node));
    }

    private static OperatorBinding binding(String node) {
        return OperatorBinding.newBuilder().setPlanNodeId(node)
                .setCapability(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef())
                .addArtifacts(ArtifactBinding.newBuilder().setArtifactId("generated")).build();
    }

    private static ReadRel read() {
        var schema = NamedStruct.newBuilder().addAllNames(List.of("id", "content", "embedding"))
                .setStruct(Type.Struct.newBuilder()
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setString(Type.String.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)))
                        .addTypes(Type.newBuilder().setList(Type.List.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)
                                .setType(Type.newBuilder().setFp32(Type.FP32.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED))))));
        return ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(TABLE)).build();
    }

    private static byte[] plan(Rel relation) {
        return Plan.newBuilder().addExtensions(io.substrait.proto.SimpleExtensionDeclaration.newBuilder()
                .setExtensionFunction(io.substrait.proto.SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                        .setFunctionAnchor(1).setName("is_not_null:any")))
                .addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(relation).addAllNames(List.of("embedding", "id", "embedding")))).build().toByteArray();
    }

    private static Expression field(int ordinal) {
        return Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
                .setRootReference(Expression.FieldReference.RootReference.newBuilder())
                .setDirectReference(Expression.ReferenceSegment.newBuilder()
                        .setStructField(Expression.ReferenceSegment.StructField.newBuilder().setField(ordinal)))).build();
    }

    private static Expression isNotNull(int ordinal) {
        return Expression.newBuilder().setScalarFunction(Expression.ScalarFunction.newBuilder()
                .setFunctionReference(1).setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                .addArguments(io.substrait.proto.FunctionArgument.newBuilder().setValue(field(ordinal)))).build();
    }
}
