package org.pangrova.planner.api.invocation;

import org.pangrova.planner.api.enums.AiInvocationOperation;
import org.pangrova.common.api.annotation.Internal;

import java.util.Locale;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Wire-neutral intent for one governed AI scalar expression. Exact model and
 * capability identities are selected from active Worker inventories at
 * dispatch time; the SQL-visible model must therefore be a literal.
 */
@Internal
public record AiInvocationPlanHint(
        String invocationId,
        String functionName,
        String modelName,
        AiInvocationOperation operation,
        boolean mediaInput) {

    public AiInvocationPlanHint {
        invocationId = requireText(invocationId, "invocationId");
        functionName = requireText(functionName, "functionName")
                .toLowerCase(Locale.ROOT);
        modelName = requireText(modelName, "modelName");
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        if (operation == AiInvocationOperation.TEXT_GENERATION && mediaInput) {
            throw new IllegalArgumentException(
                    "text generation cannot declare media input");
        }
    }
}
