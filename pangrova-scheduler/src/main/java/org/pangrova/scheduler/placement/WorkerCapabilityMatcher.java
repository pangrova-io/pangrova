package org.pangrova.scheduler.placement;

import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.scheduler.api.placement.PlacementWorker;

import java.util.List;
import java.util.Objects;

/** Exact matcher between a plan/binding requirement and Native Worker inventory. */
public final class WorkerCapabilityMatcher {

    private WorkerCapabilityMatcher() {}

    /**
     * Returns true only when a Worker advertises the same semantic capability
     * version and signature, and (when supplied) the exact immutable artifact
     * format. A plan may omit implementation_build_id to permit a rolling
     * binary upgrade; when present it is matched exactly.
     */
    public static boolean supports(
            PlacementWorker worker,
            CapabilityRef required,
            ArtifactFormatRef requiredFormat) {
        Objects.requireNonNull(worker, "worker must not be null");
        Objects.requireNonNull(required, "required capability must not be null");
        requirePlanIdentity(required);
        if (requiredFormat != null) {
            requireFormatIdentity(requiredFormat);
        }
        return worker.capabilityInventory().getCapabilitiesList().stream()
                .filter(descriptor -> matchesCapabilityRef(
                        descriptor.getRef(), required))
                .anyMatch(descriptor -> requiredFormat == null
                        || supportsFormat(descriptor, requiredFormat));
    }

    /** Returns true only when every typed operator and exact model is available. */
    public static boolean supports(
            PlacementWorker worker, List<OperatorBinding> bindings) {
        Objects.requireNonNull(worker, "worker must not be null");
        Objects.requireNonNull(bindings, "bindings must not be null");
        return bindings.stream().allMatch(binding -> {
            boolean capability;
            if (binding.hasOutputArtifact()) {
                capability = supports(worker, binding.getCapability(),
                        binding.getOutputArtifact().getFormat());
            } else if (binding.getArtifactsList().isEmpty()) {
                capability = supports(worker, binding.getCapability(), null);
            } else {
                capability = binding.getArtifactsList().stream().allMatch(artifact ->
                        supports(worker, binding.getCapability(), artifact.getFormat()));
            }
            return capability && (!binding.hasModel()
                    || supportsModel(worker, binding));
        });
    }

    private static boolean supportsModel(
            PlacementWorker worker, OperatorBinding binding) {
        var requiredModel = binding.getModel().getModel();
        if (requiredModel.getId().isBlank()
                || requiredModel.getVersion().isBlank()
                || requiredModel.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "required model identity is incomplete");
        }
        return worker.capabilityInventory().getModelsList().stream()
                .filter(availability -> availability.getModel().equals(requiredModel))
                .anyMatch(availability -> availability.getCapabilityContractsList().stream()
                        .anyMatch(contract -> matchesCapabilityRef(
                                contract.getCapability(), binding.getCapability())
                                && contract.getOutputType().equals(
                                        binding.getModel().getOutputType())));
    }

    /**
     * Matches a Worker-advertised exact implementation against a plan contract.
     * Plans may omit the build id to remain schedulable during rolling binary
     * replacement; once a build id is bound it must match exactly.
     */
    public static boolean matchesCapabilityRef(
            CapabilityRef advertised, CapabilityRef required) {
        Objects.requireNonNull(advertised, "advertised capability must not be null");
        Objects.requireNonNull(required, "required capability must not be null");
        return advertised.getCapabilityId().equals(required.getCapabilityId())
                && advertised.getSemanticVersion().equals(required.getSemanticVersion())
                && advertised.getSignatureSha256().equals(required.getSignatureSha256())
                && (required.getImplementationBuildId().isBlank()
                || advertised.getImplementationBuildId()
                        .equals(required.getImplementationBuildId()));
    }

    private static boolean supportsFormat(
            CapabilityDescriptor descriptor,
            ArtifactFormatRef required) {
        return descriptor.getSupportedArtifactFormatsList().stream()
                .anyMatch(advertised -> advertised.getFormatId().equals(required.getFormatId())
                        && advertised.getMajorVersion() == required.getMajorVersion()
                        && advertised.getMinorVersion() == required.getMinorVersion()
                        && advertised.getSchemaSha256().equals(required.getSchemaSha256())
                        && advertised.getWriterBuildId().equals(required.getWriterBuildId()));
    }

    private static void requirePlanIdentity(CapabilityRef ref) {
        if (ref.getCapabilityId().isBlank()
                || ref.getSemanticVersion().isBlank()
                || ref.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException("required capability identity is incomplete");
        }
    }

    private static void requireFormatIdentity(ArtifactFormatRef format) {
        if (format.getFormatId().isBlank()
                || format.getMajorVersion() == 0
                || format.getSchemaSha256().size() != 32
                || format.getWriterBuildId().isBlank()) {
            throw new IllegalArgumentException("required artifact format identity is incomplete");
        }
    }
}
