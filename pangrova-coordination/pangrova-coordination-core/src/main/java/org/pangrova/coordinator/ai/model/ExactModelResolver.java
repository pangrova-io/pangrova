package org.pangrova.coordinator.ai.model;

import org.pangrova.scheduler.core.failure.NoAvailableWorkerException;
import org.pangrova.scheduler.core.placement.WorkerCapabilityMatcher;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ModelAvailability;

import java.util.List;

/** Resolves a logical model name to one exact active deployment identity. */
public final class ExactModelResolver {

    private ExactModelResolver() {}

    /** Resolves one complete model serving envelope from active Worker inventories. */
    public static ModelAvailability resolve(
            List<WorkerInfo> activeWorkers,
            String modelName,
            CapabilityRef capability) {
        validateRequest(activeWorkers, modelName, capability);
        List<ModelAvailability> matches = activeWorkers.stream()
                .flatMap(worker -> worker.capabilityInventory().getModelsList().stream())
                .filter(availability -> availability.getModel().getId().equals(modelName))
                .filter(availability -> availability.getCapabilityContractsList().stream()
                        .anyMatch(contract -> WorkerCapabilityMatcher
                                .matchesCapabilityRef(
                                        contract.getCapability(), capability)))
                .distinct()
                .toList();
        if (matches.isEmpty()) {
            throw new NoAvailableWorkerException(
                    "No active Worker advertises exact model '" + modelName
                            + "' for capability " + capability.getCapabilityId());
        }
        if (matches.size() != 1) {
            throw new NoAvailableWorkerException(
                    "Model '" + modelName
                            + "' resolves to multiple active versions, signatures or limits");
        }
        ModelAvailability match = matches.get(0);
        requireCompleteEnvelope(modelName, match);
        return match;
    }

    private static void validateRequest(
            List<WorkerInfo> activeWorkers, String modelName, CapabilityRef capability) {
        if (activeWorkers == null) {
            throw new IllegalArgumentException("activeWorkers must not be null");
        }
        if (modelName == null || modelName.isBlank()
                || capability == null
                || capability.getCapabilityId().isBlank()
                || capability.getSemanticVersion().isBlank()
                || capability.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "exact model resolution requires a complete model and capability identity");
        }
    }

    private static void requireCompleteEnvelope(String modelName, ModelAvailability match) {
        if (match.getModel().getVersion().isBlank()
                || match.getModel().getSignatureSha256().size() != 32
                || match.getLimits().getMaxBatchItems() == 0
                || match.getLimits().getMaxInputTokens() == 0
                || match.getLimits().getMaxConcurrency() == 0
                || match.getLimits().getMaxProviderItemsPerTask() == 0) {
            throw new NoAvailableWorkerException(
                    "Active model '" + modelName
                            + "' advertises an incomplete serving envelope");
        }
        try {
            org.pangrova.protocol.resource.ModelResourceEnvelopes.validate(match.getLimits());
        } catch (IllegalArgumentException malformedEnvelope) {
            throw new NoAvailableWorkerException(
                    "Active model '" + modelName + "' advertises an incomplete serving envelope: "
                            + malformedEnvelope.getMessage());
        }
    }
}
