package org.pangrova.protocol;

import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorResourceRequirement;

/** One model-serving envelope projected into request constraints and task resources. */
public final class ModelResourceEnvelopes {
    private ModelResourceEnvelopes() {}

    public static void validate(CapabilityLimits limits) {
        if (limits == null || !limits.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Model serving limits are absent or contain unknown fields");
        }
        requirePositive(limits.getMaxBatchItems(), "max_batch_items");
        requirePositive(limits.getMaxConcurrency(), "max_concurrency");
        requirePositive(limits.getMaxInputTokens(), "max_input_tokens");
        requirePositive(limits.getMaxRequestBytes(), "max_request_bytes");
        requirePositive(limits.getMaxResponseBytes(), "max_response_bytes");
        requirePositive(limits.getMaxProviderItemsPerTask(), "max_provider_items_per_task");
        requirePositive(limits.getMaxProviderAttemptsPerTask(), "max_provider_attempts_per_task");
        if (limits.getMaxOutputTokens() < 0 || limits.getMaxMediaBytes() < 0) {
            throw new IllegalArgumentException("Model request limit exceeds Java accounting domain");
        }
    }

    public static ModelRequestLimits requestLimits(CapabilityLimits limits) {
        validate(limits);
        return ModelRequestLimits.newBuilder().setMaxInputTokens(limits.getMaxInputTokens())
                .setMaxOutputTokens(limits.getMaxOutputTokens()).setMaxRequestBytes(limits.getMaxRequestBytes())
                .setMaxResponseBytes(limits.getMaxResponseBytes()).build();
    }

    public static OperatorResourceRequirement resources(CapabilityLimits limits, long items, boolean media) {
        validate(limits);
        if (items <= 0 || items > limits.getMaxProviderItemsPerTask()) {
            throw new IllegalArgumentException("Logical provider items exceed the exact model task envelope");
        }
        long minimumAttempts = Math.addExact(Math.floorDiv(items - 1, limits.getMaxBatchItems()), 1);
        long attempts = limits.getMaxProviderAttemptsPerTask();
        if (attempts < minimumAttempts) {
            throw new IllegalArgumentException("Model physical attempt grant cannot cover its logical items");
        }
        return OperatorResourceRequirement.newBuilder().setProviderConcurrency(limits.getMaxConcurrency())
                .setProviderItems(items).setProviderAttempts(attempts)
                .setProviderRequestBytes(Math.multiplyExact(attempts, limits.getMaxRequestBytes()))
                .setProviderResponseBytes(Math.multiplyExact(attempts, limits.getMaxResponseBytes()))
                .setMediaBytes(media ? Math.multiplyExact(items, limits.getMaxMediaBytes()) : 0).build();
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) throw new IllegalArgumentException("Positive model serving limit required: " + name);
    }
}
