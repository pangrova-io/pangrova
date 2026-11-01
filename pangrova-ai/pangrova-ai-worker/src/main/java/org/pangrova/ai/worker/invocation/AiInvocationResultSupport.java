package org.pangrova.ai.worker.invocation;

import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.spi.ai.invocation.AiInvocationRequest;
import org.pangrova.spi.ai.invocation.AiInvocationResult;
import org.pangrova.spi.ai.invocation.InferenceResult;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Validates and preserves outputs and actual upstream identities without a usage ledger. */
final class AiInvocationResultSupport {
    private AiInvocationResultSupport() {}

    static AiInvocationResult inferenceResult(
            AiInvocationRequest request, List<InferenceResult> results) {
        List<AiInvocationResult.Output> outputs =
                results.stream()
                        .map(
                                value ->
                                        (AiInvocationResult.Output)
                                                new AiInvocationResult.TextOutput(value.text()))
                        .toList();
        var attributes = new java.util.LinkedHashMap<String, String>();
        for (int ordinal = 0; ordinal < results.size(); ordinal++) {
            String id = results.get(ordinal).providerRequestId();
            if (!id.isEmpty()) {
                attributes.put("provider_request_" + ordinal, id);
            }
        }
        return new AiInvocationResult(
                request.requestId(),
                request.modelName(),
                request.modelVersion(),
                outputs,
                results.size() == 1 ? results.get(0).providerRequestId() : "",
                AiInvocationResult.FinishReason.COMPLETE,
                attributes);
    }

    static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    static <T> void validateResultCount(
            AiInvocationRequest request, List<T> results, int expectedCount) {
        if (results == null
                || results.size() != expectedCount
                || results.stream().anyMatch(Objects::isNull)) {
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_RESPONSE_INVALID,
                    false,
                    "Provider returned invalid result cardinality for request "
                            + request.requestId()
                            + ": expected "
                            + expectedCount
                            + ", got "
                            + (results == null ? "null" : results.size()));
        }
    }
}
