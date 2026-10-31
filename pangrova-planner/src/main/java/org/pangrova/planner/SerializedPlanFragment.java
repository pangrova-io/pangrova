package org.pangrova.planner;

import java.util.List;

import org.pangrova.common.api.annotation.Internal;

/** Serialized Substrait fragment plus typed relation intents to bind before dispatch. */
@Internal
public record SerializedPlanFragment(
        byte[] planBytes,
        List<RetrievalTopKPlanHint> retrievalTopKHints,
        List<AiInvocationPlanHint> aiInvocationHints) {

    public SerializedPlanFragment {
        planBytes = planBytes.clone();
        retrievalTopKHints = List.copyOf(retrievalTopKHints);
        aiInvocationHints = List.copyOf(aiInvocationHints);
    }

    public SerializedPlanFragment(
            byte[] planBytes,
            List<RetrievalTopKPlanHint> retrievalTopKHints) {
        this(planBytes, retrievalTopKHints, List.of());
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }
}
