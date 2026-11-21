package org.pangrova.protocol.retrieval;

import io.substrait.proto.Expression;
import org.pangrova.common.api.annotation.Internal;

/** Shared direct-field expression utilities for retrieval plan binding. */
@Internal
final class SubstraitFieldReference {

    private SubstraitFieldReference() {}

    static Expression fieldReference(int fieldIndex) {
        return org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(fieldIndex);
    }

    static int directFieldIndex(Expression expression) {
        if (!expression.hasSelection() || !expression.getSelection().hasRootReference()
                || !expression.getSelection().hasDirectReference()
                || !expression.getSelection().getDirectReference().hasStructField()
                || expression.getSelection().getDirectReference().getStructField().hasChild()) {
            return -1;
        }
        return expression.getSelection().getDirectReference().getStructField().getField();
    }
}
