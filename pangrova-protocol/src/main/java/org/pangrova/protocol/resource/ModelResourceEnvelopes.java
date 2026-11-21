package org.pangrova.protocol.resource;

import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorResourceRequirement;

/** One model-serving envelope projected into request constraints and task resources. */
public final class ModelResourceEnvelopes {
    private ModelResourceEnvelopes() {}

    /**
     * Validates the explicit serving limits within Java's signed accounting domain.
     *
     * @param limits model-serving limits advertised by the capability inventory
     * @throws IllegalArgumentException if limits are absent, unknown, nonpositive, or outside that domain
     */
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

    /**
     * Projects validated serving limits into per-request token and body constraints.
     *
     * @param limits exact model-serving capability limits
     * @return canonical request limits without deriving body budgets from token limits
     * @throws IllegalArgumentException if serving limits are invalid
     */
    public static ModelRequestLimits requestLimits(CapabilityLimits limits) {
        validate(limits);
        return ModelRequestLimits.newBuilder().setMaxInputTokens(limits.getMaxInputTokens())
                .setMaxOutputTokens(limits.getMaxOutputTokens()).setMaxRequestBytes(limits.getMaxRequestBytes())
                .setMaxResponseBytes(limits.getMaxResponseBytes()).build();
    }

    /**
     * Derives checked task resources from logical items and the explicit physical-attempt envelope.
     *
     * @param limits exact model-serving capability limits
     * @param items logical provider items assigned to the task
     * @param media whether the task consumes the declared per-item media budget
     * @return canonical task concurrency, item, attempt, body-byte, and media requirements
     * @throws IllegalArgumentException if limits or items are invalid or attempts cannot cover the items
     * @throws ArithmeticException if cumulative byte accounting overflows
     */
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
