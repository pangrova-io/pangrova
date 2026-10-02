package org.pangrova.protocol.retrieval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RetrievalSourceDetail;
import org.pangrova.protocol.v2.RetryClass;

/** Mutable traversal state scoped to one Top-K plan-binding operation. */
final class RetrievalTopKBindingCursor {
    private final List<RetrievalTopKBinding> bindings;
    private final String idPrefix;
    private final Map<Integer, String> functionNames;
    private final List<OperatorBinding> operatorBindings = new ArrayList<>();
    private int index;

    RetrievalTopKBindingCursor(
            List<RetrievalTopKBinding> bindings,
            String idPrefix,
            Map<Integer, String> functionNames) {
        this.bindings = List.copyOf(bindings);
        if (idPrefix == null || idPrefix.isBlank()) {
            throw new IllegalArgumentException("Top-K relation id prefix is required");
        }
        this.idPrefix = idPrefix;
        this.functionNames = Map.copyOf(functionNames);
    }

    boolean hasNext() {
        return index < bindings.size();
    }

    RetrievalTopKBinding peek() {
        return bindings.get(index);
    }

    int boundCount() {
        return index;
    }

    String nextNodeId() {
        return idPrefix + "-topk-" + index;
    }

    Map<Integer, String> functionNames() {
        return functionNames;
    }

    List<OperatorBinding> operatorBindings() {
        return List.copyOf(operatorBindings);
    }

    void advance(RetrievalSourceDetail detail) {
        RetrievalTopKBinding binding = bindings.get(index);
        var policy = ExecutionPolicy.newBuilder()
                .setCoveragePolicy(CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                .setRetryClass(RetryClass.RETRY_CLASS_IDEMPOTENT)
                .setApproximation(org.pangrova.protocol.v2.ApproximationProfile
                        .newBuilder()
                        .setProfileId(binding.approximationProfileId())
                        .setCandidateLimit(detail.getCandidateLimit())
                        .setExactRescoreLimit(binding.exact()
                                ? detail.getCandidateLimit()
                                : detail.getExactRescoreLimit())
                        .setRequiredRecallAtK(binding.requiredRecallAtK())
                        .setRecallK(binding.recallK()));
        operatorBindings.add(OperatorBinding.newBuilder()
                .setPlanNodeId(detail.getPlanNodeId())
                .setCapability(detail.getInvocation().getCapability())
                .addAllArtifacts(binding.artifacts())
                .setResources(org.pangrova.protocol.v2
                        .OperatorResourceRequirement.newBuilder()
                        .setCandidateRows(detail.getCandidateLimit()))
                .setExecutionPolicy(policy)
                .build());
        index++;
    }
}
