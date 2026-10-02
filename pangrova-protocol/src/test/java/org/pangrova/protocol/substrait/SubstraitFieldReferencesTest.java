package org.pangrova.protocol.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Wire/type-authority regression; actual Native execution is verified separately. */
class SubstraitFieldReferencesTest {
    private static final Type I64 = Type.newBuilder().setI64(Type.I64.newBuilder()
            .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    private static final NamedStruct INPUT = NamedStruct.newBuilder().addNames("value")
            .setStruct(Type.Struct.newBuilder().addTypes(I64)).build();

    @Test
    void directReferencesSerializeAnExplicitRootAndPreserveDeclaredType() throws Exception {
        var field = Expression.parseFrom(SubstraitRelationLayout.rootFieldReference(0).toByteArray());
        assertThat(field.getSelection().hasRootReference()).isTrue();
        assertThat(SubstraitRelationLayout.typeOf(field, INPUT)).isEqualTo(I64);
        assertThatThrownBy(() -> SubstraitRelationLayout.rootFieldReference(-1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Negative");
        assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(
                SubstraitRelationLayout.rootFieldReference(1), INPUT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("out of bounds");
    }

    @Test
    void missingAndUnsupportedOriginsNeverImplicitlySelectTheInput() {
        var direct = SubstraitRelationLayout.rootFieldReference(0).getSelection();
        for (var malformed : List.of(direct.toBuilder().clearRootReference().build(),
                direct.toBuilder().setOuterReference(Expression.FieldReference.OuterReference.newBuilder()
                        .setStepsOut(1)).build(),
                direct.toBuilder().setLambdaParameterReference(
                        Expression.FieldReference.LambdaParameterReference.newBuilder()).build())) {
            assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(
                    Expression.newBuilder().setSelection(malformed).build(), INPUT))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit supported root");
        }
    }

    @Test
    void rootRequiresStructSelectionWhileExpressionRootKeepsItsOwnType() {
        var element = Expression.ReferenceSegment.newBuilder().setListElement(
                Expression.ReferenceSegment.ListElement.newBuilder().setOffset(0));
        var root = Expression.FieldReference.newBuilder().setRootReference(
                Expression.FieldReference.RootReference.getDefaultInstance()).setDirectReference(element);
        assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(
                Expression.newBuilder().setSelection(root).build(), INPUT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("struct_field");
        var list = Expression.newBuilder().setNested(Expression.Nested.newBuilder().setList(
                Expression.Nested.List.newBuilder().addValues(Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setI64(9))))).build();
        var expressionRoot = Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
                .setExpression(list).setDirectReference(element)).build();
        assertThat(SubstraitRelationLayout.typeOf(expressionRoot, INPUT).getI64().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
    }

    @Test
    void rootLabelsRequireTheCompleteDeclaredNestedDfsShape() {
        var nested = Type.newBuilder().setList(Type.List.newBuilder().setType(
                Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(I64)))).build();
        var input = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(
                NamedStruct.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(nested))
                        .addNames("input").addNames("nested"))).build();
        var root = RelRoot.newBuilder().setInput(input).addNames("result").addNames("child").build();
        assertThat(SubstraitRelationLayout.rootSchema(root).getNamesList())
                .containsExactly("result", "child");
        assertThatThrownBy(() -> SubstraitRelationLayout.rootSchema(root.toBuilder()
                .clearNames().addNames("result").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing DFS");
        assertThatThrownBy(() -> SubstraitRelationLayout.rootSchema(root.toBuilder()
                .addNames("surplus").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unused DFS");
    }
}
