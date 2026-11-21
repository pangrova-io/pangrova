package org.pangrova.protocol.retrieval;

import io.substrait.proto.Expression;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SubstraitFieldReferenceTest {
    @Test
    void onlyDirectIncomingFieldsCanBeReboundByOrdinal() {
        var incoming = SubstraitFieldReference.fieldReference(3);
        assertThat(incoming.getSelection().hasRootReference()).isTrue();
        assertThat(SubstraitFieldReference.directFieldIndex(incoming)).isEqualTo(3);
        var missing = incoming.toBuilder().setSelection(incoming.getSelection().toBuilder()
                .clearRootReference()).build();
        assertThat(SubstraitFieldReference.directFieldIndex(missing)).isEqualTo(-1);
        var expressionRoot = incoming.toBuilder().setSelection(incoming.getSelection().toBuilder()
                .setExpression(Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(7)))).build();
        assertThat(SubstraitFieldReference.directFieldIndex(expressionRoot)).isEqualTo(-1);
        var nested = incoming.toBuilder().setSelection(incoming.getSelection().toBuilder()
                .setDirectReference(incoming.getSelection().getDirectReference().toBuilder().setStructField(
                        incoming.getSelection().getDirectReference().getStructField().toBuilder().setChild(
                                Expression.ReferenceSegment.newBuilder().setStructField(
                                        Expression.ReferenceSegment.StructField.newBuilder().setField(1)))))).build();
        assertThat(SubstraitFieldReference.directFieldIndex(nested)).isEqualTo(-1);
    }
}
