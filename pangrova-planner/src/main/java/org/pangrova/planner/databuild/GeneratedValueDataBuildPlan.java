package org.pangrova.planner.databuild;

import org.pangrova.planner.AiInvocationPlanHint;
import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;

/** Canonical background plan plus the governed model invocation it contains. */
@Internal
public record GeneratedValueDataBuildPlan(
        DataBuildPlan artifact,
        AiInvocationPlanHint modelInvocation) {

    public GeneratedValueDataBuildPlan {
        artifact = Objects.requireNonNull(artifact, "artifact must not be null");
        modelInvocation = Objects.requireNonNull(
                modelInvocation, "modelInvocation must not be null");
        if (modelInvocation.operation() != AiInvocationPlanHint.Operation.EMBEDDING) {
            throw new IllegalArgumentException(
                    "generated vector values require an embedding invocation");
        }
    }
}
