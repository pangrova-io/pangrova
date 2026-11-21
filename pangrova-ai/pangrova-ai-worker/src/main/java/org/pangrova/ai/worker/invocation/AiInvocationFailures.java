package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;

/** Central failure classification for the Worker AI invocation path. */
final class AiInvocationFailures {

    private AiInvocationFailures() {}

    static AiInvocationException modelNotFound(AiInvocationRequest request) {
        return new AiInvocationException(
                AiFailureCategory.MODEL_NOT_FOUND,
                false,
                "No provider registered for exact model "
                        + request.modelName() + "@" + request.modelVersion());
    }

    static AiInvocationException invalidInput(
            AiInvocationRequest request, String detail) {
        return new AiInvocationException(
                AiFailureCategory.INVALID_REQUEST,
                false,
                "Invalid AI invocation request " + request.requestId() + ": " + detail);
    }

    static AiInvocationException classify(RuntimeException failure) {
        Throwable cause = unwrapCompletionFailure(failure);
        AiInvocationException known = classifyKnown(cause, failure);
        return known != null ? known : classifyGeneric(cause, failure);
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable cause = failure;
        while (cause instanceof java.util.concurrent.CompletionException
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static AiInvocationException classifyKnown(
            Throwable cause, RuntimeException failure) {
        if (cause instanceof AiInvocationException classified) {
            return classified;
        }
        if (cause instanceof PangrovaException pangrova) {
            AiFailureCategory category = map(pangrova.getErrorCode());
            if (category != null) {
                return new AiInvocationException(
                        category,
                        category == AiFailureCategory.PROVIDER_UNAVAILABLE,
                        pangrova.getMessage(),
                        failure);
            }
        }
        return null;
    }

    private static AiInvocationException classifyGeneric(
            Throwable cause, RuntimeException failure) {
        String message = failure.getMessage() == null
                ? failure.getClass().getSimpleName()
                : failure.getMessage();
        if (cause instanceof IllegalArgumentException
                || cause instanceof ArithmeticException) {
            return new AiInvocationException(
                    AiFailureCategory.PROVIDER_RESPONSE_INVALID,
                    false,
                    message,
                    failure);
        }
        if (cause instanceof SecurityException) {
            return new AiInvocationException(
                    AiFailureCategory.AUTHORIZATION,
                    false,
                    message,
                    failure);
        }
        if (cause instanceof IllegalStateException) {
            return new AiInvocationException(
                    AiFailureCategory.INTERNAL,
                    false,
                    message,
                    failure);
        }
        return new AiInvocationException(
                AiFailureCategory.PROVIDER_UNAVAILABLE,
                true,
                message,
                failure);
    }

    private static AiFailureCategory map(PangrovaErrorCode errorCode) {
        return switch (errorCode) {
            case AI_INVOCATION_AUTHORIZATION_DENIED,
                    AI_EMBEDDING_ACCESS_DENIED,
                    ACCESS_DENIED -> AiFailureCategory.AUTHORIZATION;
            case AI_EMBEDDING_DIMENSION_MISMATCH ->
                    AiFailureCategory.PROVIDER_RESPONSE_INVALID;
            case AI_BLOB_SIZE_EXCEEDED -> AiFailureCategory.BUDGET_EXCEEDED;
            case AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    AI_INFERENCE_PROVIDER_UNAVAILABLE,
                    AI_MODEL_SERVICE_UNAVAILABLE ->
                    AiFailureCategory.PROVIDER_UNAVAILABLE;
            default -> null;
        };
    }
}
