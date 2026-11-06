package org.pangrova.planner.api.invocation;

import org.pangrova.common.api.annotation.Internal;

import java.util.Locale;

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
        Operation operation,
        boolean mediaInput) {

    public enum Operation {
        TEXT_GENERATION,
        MULTIMODAL_GENERATION,
        EMBEDDING
    }

    public AiInvocationPlanHint {
        invocationId = requireText(invocationId, "invocationId");
        functionName = requireText(functionName, "functionName")
                .toLowerCase(Locale.ROOT);
        modelName = requireText(modelName, "modelName");
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        if (operation == Operation.TEXT_GENERATION && mediaInput) {
            throw new IllegalArgumentException(
                    "text generation cannot declare media input");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
