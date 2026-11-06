package org.pangrova.planner.core.substrait;

import io.substrait.plan.Plan;
import org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint;
import org.pangrova.planner.api.invocation.AiInvocationPlanHint;

import java.util.List;

/**
 * Immutable result of a single {@link SubstraitConverter} conversion.
 *
 * @param plan the converted Substrait plan
 * @param retrievalTopKHints typed retrieval intents discovered in this fragment
 */
public record ConversionResult(
        Plan plan,
        List<RetrievalTopKPlanHint> retrievalTopKHints,
        List<AiInvocationPlanHint> aiInvocationHints) {
    /** Freezes invocation/retrieval intents before downstream binding. */
    public ConversionResult {
        retrievalTopKHints = List.copyOf(retrievalTopKHints);
        aiInvocationHints = List.copyOf(aiInvocationHints);
    }
}
