package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.planner.api.invocation.AiInvocationPlanHint;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Owns per-conversion invocation identities and exact literal model bindings. */
final class AiInvocationBinder {
    private final Consumer<AiInvocationPlanHint> sink;
    private int nextInvocationId;

    AiInvocationBinder(Consumer<AiInvocationPlanHint> sink) {
        this.sink = java.util.Objects.requireNonNull(sink, "aiInvocationSink must not be null");
    }

    List<Expression> bind(FunctionReference reference, RexCall call, String opName, List<Expression> publicArguments) {
        if (!FunctionBindingDeclarations.hasGovernedArguments("SCALAR", reference.namespace(), reference.name())) {
            return publicArguments;
        }
        if (call.getOperands().isEmpty()
                || !(call.getOperands().get(0) instanceof RexLiteral modelLiteral)) {
            throw new SubstraitConversionException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    opName + " requires a literal exact model name as its first argument");
        }
        String modelName = modelLiteral.getValueAs(String.class);
        if (modelName == null || modelName.isBlank()) {
            throw new SubstraitConversionException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    opName + " model name must not be blank");
        }
        boolean mediaInput = call.getOperands().stream().skip(1).anyMatch(operand ->
                operand.getType().getSqlTypeName() == SqlTypeName.ROW);
        String invocationId = "ai-invoke-" + nextInvocationId++;
        sink.accept(new AiInvocationPlanHint(invocationId, opName, modelName,
                operation(opName, mediaInput), mediaInput));
        List<Expression> internalArguments = new ArrayList<>(publicArguments);
        internalArguments.add(ExpressionCreator.string(false, invocationId));
        return List.copyOf(internalArguments);
    }

    private static AiInvocationPlanHint.Operation operation(String opName, boolean mediaInput) {
        if ("AI_EMBED".equals(opName)) {
            return AiInvocationPlanHint.Operation.EMBEDDING;
        }
        boolean dedicatedMultimodal = switch (opName) {
            case "AI_DESCRIBE_IMAGE", "AI_PARSE_DOCUMENT", "AI_TRANSCRIBE", "AI_OCR" -> true;
            default -> false;
        };
        return mediaInput || dedicatedMultimodal
                ? AiInvocationPlanHint.Operation.MULTIMODAL_GENERATION
                : AiInvocationPlanHint.Operation.TEXT_GENERATION;
    }
}
