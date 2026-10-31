package org.pangrova.coordinator.query.execution;

import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.PolicySnapshot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable server-compiled relation; never constructed from client execution bytes. */
public record AuthorizedRelation(
        byte[] planBytes,
        List<SplitSourceDescriptor> sources,
        List<String> outputNames,
        List<String> outputTypeNames,
        String effectivePlanDigest,
        boolean splitIndependent,
        Map<String, byte[]> sourceSchemas,
        Map<String, PolicySnapshot> observedPolicies,
        AuthorizedStageGraph stageGraph) {

    public AuthorizedRelation {
        planBytes = Objects.requireNonNull(planBytes, "planBytes").clone();
        sources = List.copyOf(sources);
        outputNames = List.copyOf(outputNames);
        outputTypeNames = List.copyOf(outputTypeNames);
        Objects.requireNonNull(effectivePlanDigest, "effectivePlanDigest");
        sourceSchemas = copySchemas(sourceSchemas);
        observedPolicies = Map.copyOf(observedPolicies);
        Objects.requireNonNull(stageGraph, "stageGraph");
        if (planBytes.length == 0 || outputNames.size() != outputTypeNames.size()) {
            throw new IllegalArgumentException("Authorized relation has an invalid plan or output schema");
        }
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }

    @Override
    public Map<String, byte[]> sourceSchemas() {
        return copySchemas(sourceSchemas);
    }

    private static Map<String, byte[]> copySchemas(Map<String, byte[]> schemas) {
        Map<String, byte[]> result = new HashMap<>();
        schemas.forEach((table, schema) -> result.put(table, schema.clone()));
        return Map.copyOf(result);
    }
}
