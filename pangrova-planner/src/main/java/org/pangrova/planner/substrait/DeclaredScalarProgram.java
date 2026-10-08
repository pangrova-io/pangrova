package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import org.pangrova.protocol.function.declaration.DeclaredFunctionRewrite;

import java.util.ArrayList;
import java.util.List;

/** Lowers the declaration's ordered predicate program before serializing any function reference. */
final class DeclaredScalarProgram {
    private DeclaredScalarProgram() {}

    static Expression lower(DeclaredFunctionRewrite rewrite, List<Expression> inputs,
                            Type output, FunctionMapper mapper) {
        if (!(output instanceof Type.Bool)) {
            throw new IllegalArgumentException(
                    "Declared predicate program requires its validator-derived BOOLEAN output");
        }
        List<Expression> evaluated = new ArrayList<>();
        for (var node : rewrite.nodes()) {
            var source = node.inputs().isEmpty() ? evaluated : inputs;
            var ordinals = node.inputs().isEmpty() ? node.nodes() : node.inputs();
            List<Expression> arguments = new ArrayList<>();
            for (int ordinal : ordinals) {
                if (ordinal >= source.size()) throw new IllegalArgumentException("Declared rewrite input is absent");
                arguments.add(source.get(ordinal));
            }
            var target = new FunctionReference(node.target().namespace(), node.target().name());
            evaluated.add(ImmutableExpression.ScalarFunctionInvocation.builder()
                    .declaration(mapper.resolveScalarVariant(target)).outputType(output).arguments(arguments).build());
        }
        return evaluated.get(rewrite.result());
    }
}
