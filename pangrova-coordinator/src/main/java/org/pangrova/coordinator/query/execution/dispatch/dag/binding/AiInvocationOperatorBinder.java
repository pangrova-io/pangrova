package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import org.pangrova.spi.annotation.Internal;
import org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector;

import org.pangrova.planner.AiInvocationPlanHint;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.ExecutionReusePolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.RetryClass;
import org.pangrova.spi.security.UserIdentity;

import java.util.Objects;
import java.util.List;

import org.pangrova.coordinator.worker.WorkerCapabilityMatcher;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.ModelResourceEnvelopes;

/** One control-plane compiler for governed AI expression bindings. */
@Internal
public final class AiInvocationOperatorBinder {

    private static final String SERVICE_ID = "worker-local-governed-provider";
    private static final String ROUTE_POLICY_VERSION = "exact-worker-inventory";
    private static final long REUSE_TTL_MS = 3_600_000L;
    private static final long REUSE_MAX_ENTRIES = 100_000L;
    private static final long REUSE_MAX_BYTES = 512L * 1024 * 1024;
    private static final int REUSE_KEY_DIMENSION = 256;
    private static final double REUSE_SIMILARITY_THRESHOLD = 0.98;

    private AiInvocationOperatorBinder() {}

    /** Builds a complete foreground-query binding. */
    public static OperatorBinding bindQuery(
            AiInvocationPlanHint hint,
            WorkerSelector workerSelector,
            UserIdentity user) {
        OperatorBinding template = bindTemplate(hint, workerSelector, user);
        return template.toBuilder()
                .setExecutionPolicy(ExecutionPolicy.newBuilder()
                        .setCoveragePolicy(
                                CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                        .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                        .setRetryClass(
                                RetryClass.RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY)
                        .setIdempotencyKey(hint.invocationId()))
                .build();
    }

    /**
     * Builds the immutable binding template persisted with a durable plan.
     * The durable dispatcher adds a page-scoped deadline and idempotency key
     * after claiming a fenced attempt.
     */
    public static OperatorBinding bindTemplate(
            AiInvocationPlanHint hint,
            WorkerSelector workerSelector,
            UserIdentity user) {
        Objects.requireNonNull(hint, "hint must not be null");
        Objects.requireNonNull(workerSelector, "workerSelector must not be null");
        Objects.requireNonNull(user, "user must not be null");
        CapabilityRef capability = capability(hint.operation());
        ModelAvailability availability = workerSelector.resolveExactModel(
                hint.modelName(), capability, user);
        ModelCapabilityContract modelContract = modelContract(
                availability, capability, hint.modelName());
        if (hint.operation() == AiInvocationPlanHint.Operation.EMBEDDING) {
            VectorLogicalType.requireDimension(modelContract.getOutputType());
        }
        CapabilityLimits limits = availability.getLimits();
        validateLimits(hint, limits);

        OperatorResourceRequirement resources = ModelResourceEnvelopes.resources(
                limits, limits.getMaxProviderItemsPerTask(), hint.mediaInput());
        OperatorBinding.Builder binding = OperatorBinding.newBuilder()
                .setPlanNodeId(hint.invocationId())
                .setCapability(capability)
                .setModel(ModelBinding.newBuilder()
                        .setModel(availability.getModel())
                        .setServiceId(SERVICE_ID)
                        .setRoutePolicyVersion(ROUTE_POLICY_VERSION)
                        .setOutputType(modelContract.getOutputType())
                        .setRequestLimits(ModelResourceEnvelopes.requestLimits(limits)))
                .setResources(resources);
        if (hint.operation() == AiInvocationPlanHint.Operation.TEXT_GENERATION) {
            binding.setReusePolicy(ExecutionReusePolicy.newBuilder()
                    .setEnabled(true)
                    .setTtlMs(REUSE_TTL_MS)
                    .setMaxEntries(REUSE_MAX_ENTRIES)
                    .setMaxBytes(REUSE_MAX_BYTES)
                    .setSimilarityThreshold(REUSE_SIMILARITY_THRESHOLD)
                    .setKeyDimension(REUSE_KEY_DIMENSION)
                    .setRequireComplete(true));
        }
        return binding.build();
    }

    private static ModelCapabilityContract modelContract(
            ModelAvailability availability,
            CapabilityRef capability,
            String modelName) {
        List<ModelCapabilityContract> matches = availability
                .getCapabilityContractsList().stream()
                .filter(contract -> WorkerCapabilityMatcher.matchesCapabilityRef(
                        contract.getCapability(), capability))
                .toList();
        if (matches.size() != 1) {
            throw new IllegalArgumentException(
                    "model must advertise one exact output contract: " + modelName);
        }
        return matches.get(0);
    }

    private static CapabilityRef capability(AiInvocationPlanHint.Operation operation) {
        return switch (operation) {
            case TEXT_GENERATION -> BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
            case MULTIMODAL_GENERATION ->
                    BuiltInCapabilityCatalog.aiMultimodalGenerationPlanRef();
            case EMBEDDING -> BuiltInCapabilityCatalog.aiEmbeddingPlanRef();
        };
    }

    private static void validateLimits(
            AiInvocationPlanHint hint, CapabilityLimits limits) {
        ModelResourceEnvelopes.validate(limits);
        if (hint.operation() != AiInvocationPlanHint.Operation.EMBEDDING
                && limits.getMaxOutputTokens() == 0) {
            throw new IllegalArgumentException(
                    "generation model has no positive output-token limit: "
                            + hint.modelName());
        }
        if (hint.mediaInput() && limits.getMaxMediaBytes() == 0) {
            throw new IllegalArgumentException(
                    "multimodal model has no positive media-byte limit: "
                            + hint.modelName());
        }
    }
}
