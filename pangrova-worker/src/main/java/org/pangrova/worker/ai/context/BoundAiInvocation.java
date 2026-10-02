package org.pangrova.worker.ai.context;

import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiRequestConstraints;

import java.util.Arrays;

/** Exact control-plane binding for one AI expression invocation. */
public record BoundAiInvocation(
        String invocationId,
        String modelId,
        String modelVersion,
        byte[] modelSignatureSha256,
        AiCapabilityDescriptor.Operation operation,
        String idempotencyBase,
        AiRequestConstraints requestConstraints,
        int maxProviderConcurrency,
        long maxProviderItems,
        long maxProviderAttempts,
        long maxProviderRequestBytes,
        long maxProviderResponseBytes,
        long maxMediaBytes,
        int outputVectorDimension) {

    private static final int MAX_VECTOR_DIMENSION = 65_535;

    public BoundAiInvocation {
        invocationId = requireText(invocationId, "invocationId");
        modelId = requireText(modelId, "modelId");
        modelVersion = requireText(modelVersion, "modelVersion");
        requireSignature(modelSignatureSha256);
        modelSignatureSha256 = modelSignatureSha256.clone();
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        idempotencyBase = requireText(idempotencyBase, "idempotencyBase");
        java.util.Objects.requireNonNull(requestConstraints, "requestConstraints required");
        requireEnvelope(maxProviderConcurrency, maxProviderItems, maxProviderAttempts,
                maxProviderRequestBytes, maxProviderResponseBytes, maxMediaBytes);
        if (operation == AiCapabilityDescriptor.Operation.EMBEDDING) {
            if (outputVectorDimension < 1
                    || outputVectorDimension > MAX_VECTOR_DIMENSION) {
                throw new IllegalArgumentException(
                        "embedding outputVectorDimension must be in [1, 65535]");
            }
        } else if (outputVectorDimension != 0) {
            throw new IllegalArgumentException(
                    "non-embedding invocation must not declare a vector dimension");
        }
    }

    private static void requireSignature(byte[] modelSignatureSha256) {
        if (modelSignatureSha256 == null || modelSignatureSha256.length != 32) {
            throw new IllegalArgumentException(
                    "modelSignatureSha256 must contain exactly 32 bytes");
        }
    }

    private static void requireEnvelope(
            int maxProviderConcurrency, long maxProviderItems, long maxProviderAttempts,
            long maxProviderRequestBytes, long maxProviderResponseBytes,
            long maxMediaBytes) {
        if (maxProviderConcurrency <= 0
                || maxProviderItems <= 0 || maxProviderAttempts <= 0
                || maxProviderRequestBytes <= 0 || maxProviderResponseBytes <= 0
                || maxMediaBytes < 0) {
            throw new IllegalArgumentException(
                    "operator provider resource envelope is invalid");
        }
    }

    @Override
    public byte[] modelSignatureSha256() {
        return modelSignatureSha256.clone();
    }

    @Override
    public boolean equals(Object value) {
        return value instanceof BoundAiInvocation other
                && sameProviderIdentity(other)
                && sameResourceEnvelope(other);
    }

    private boolean sameProviderIdentity(BoundAiInvocation other) {
        return invocationId.equals(other.invocationId)
                && modelId.equals(other.modelId)
                && modelVersion.equals(other.modelVersion)
                && Arrays.equals(
                        modelSignatureSha256, other.modelSignatureSha256)
                && operation == other.operation
                && requestConstraints.equals(other.requestConstraints)
                && idempotencyBase.equals(other.idempotencyBase);
    }

    private boolean sameResourceEnvelope(BoundAiInvocation other) {
        return maxProviderConcurrency == other.maxProviderConcurrency
                && maxProviderItems == other.maxProviderItems
                && maxProviderAttempts == other.maxProviderAttempts
                && maxProviderRequestBytes == other.maxProviderRequestBytes
                && maxProviderResponseBytes == other.maxProviderResponseBytes
                && maxMediaBytes == other.maxMediaBytes
                && outputVectorDimension == other.outputVectorDimension;
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(
                invocationId, modelId, modelVersion, operation,
                idempotencyBase, requestConstraints, maxProviderConcurrency,
                maxProviderItems, maxProviderAttempts, maxProviderRequestBytes,
                maxProviderResponseBytes, maxMediaBytes, outputVectorDimension);
        return 31 * result + Arrays.hashCode(modelSignatureSha256);
    }

    /** Rejects a provider whose immutable execution identity differs from the binding. */
    public void requireExactProvider(
            String actualModelId,
            String actualVersion,
            byte[] actualSignature,
            AiCapabilityDescriptor.Operation actualOperation) {
        if (!modelId.equals(actualModelId)
                || !modelVersion.equals(actualVersion)
                || actualSignature == null
                || !Arrays.equals(modelSignatureSha256, actualSignature)
                || operation != actualOperation) {
            throw new org.pangrova.spi.ai.AiInvocationException(
                    org.pangrova.spi.ai.AiInvocationException.Category.CAPABILITY_MISMATCH,
                    false,
                    "Provider execution identity differs from the exact OperatorBinding");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
