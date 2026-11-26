package org.pangrova.planner.analysis;

import io.substrait.proto.Plan;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.api.invocation.AiInvocationPlanHint;
import org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint;

import java.util.List;
import java.util.Objects;

/** The canonical in-memory Substrait plan through live stage preparation. */
@Internal
public record CompiledStagePlan(Plan plan, List<RetrievalTopKPlanHint> retrievalTopKHints,
                                List<AiInvocationPlanHint> aiInvocationHints) {
    public CompiledStagePlan {
        Objects.requireNonNull(plan, "plan");
        retrievalTopKHints = List.copyOf(retrievalTopKHints);
        aiInvocationHints = List.copyOf(aiInvocationHints);
    }

    /** Keeps the compiler's operation intents when the runtime binds an exchange. */
    public CompiledStagePlan withPlan(Plan boundPlan) {
        return new CompiledStagePlan(boundPlan, retrievalTopKHints, aiInvocationHints);
    }

    /** Materializes wire bytes at the persisted or transport boundary. */
    public SerializedPlanFragment serialize() {
        return new SerializedPlanFragment(plan.toByteArray(), retrievalTopKHints, aiInvocationHints);
    }
}
