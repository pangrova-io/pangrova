package org.pangrova.ai.api.invocation;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** Generated text and the upstream request identity, without LLM usage statistics. */
@Experimental
public record InferenceResult(String text, String providerRequestId) {
    public InferenceResult {
        Objects.requireNonNull(text, "text required");
        Objects.requireNonNull(providerRequestId, "providerRequestId required");
    }

    /** Result for a provider that does not return an upstream request identifier. */
    public static InferenceResult of(String text) { return new InferenceResult(text, ""); }
}
