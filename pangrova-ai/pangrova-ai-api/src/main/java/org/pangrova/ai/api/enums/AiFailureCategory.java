package org.pangrova.ai.api.enums;

/** Failure classification shared by providers, retry decisions and audit. */
public enum AiFailureCategory {
    INVALID_REQUEST,
    MODEL_NOT_FOUND,
    MODEL_DISABLED,
    CAPABILITY_MISMATCH,
    AUTHENTICATION,
    AUTHORIZATION,
    RATE_LIMIT,
    TIMEOUT,
    PROVIDER_UNAVAILABLE,
    PROVIDER_OUTCOME_UNCERTAIN,
    PROVIDER_RESPONSE_INVALID,
    CONTENT_REJECTED,
    BUDGET_EXCEEDED,
    INTERNAL
}
