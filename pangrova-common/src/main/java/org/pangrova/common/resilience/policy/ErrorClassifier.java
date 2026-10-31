package org.pangrova.common.resilience.policy;

import org.pangrova.common.exception.PangrovaErrorCode;

import java.util.Map;

/**
 * Classifies error codes into categories and determines retry eligibility.
 *
 * <p>Classification relies on the error code naming convention defined in
 * {@link org.pangrova.common.exception.PangrovaErrorCode}. Each error code follows
 * the format {@code CATEGORY-NNNN} (e.g. {@code RESOURCE-0001}). The prefix
 * before the first hyphen is matched against a known prefix map to determine
 * the {@link ErrorCategory}. Unknown prefixes default to
 * {@link ErrorCategory#INTERNAL_ERROR}.</p>
 *
 * <p>Recognized prefixes and their categories:</p>
 * <ul>
 *   <li>{@code USER}, {@code VALIDATION} &rarr; {@link ErrorCategory#USER_ERROR}</li>
 *   <li>{@code RESOURCE}, {@code MEMORY} &rarr; {@link ErrorCategory#RESOURCE_ERROR}</li>
 *   <li>{@code INTERNAL}, {@code CONFIG} &rarr; {@link ErrorCategory#INTERNAL_ERROR}</li>
 *   <li>{@code EXTERNAL}, {@code METADATA}, {@code NATIVE} &rarr; {@link ErrorCategory#EXTERNAL_ERROR}</li>
 *   <li>{@code EXCHANGE}, {@code FLIGHT} &rarr; {@link ErrorCategory#EXCHANGE_ERROR}</li>
 * </ul>
 */
public class ErrorClassifier {

    public enum ErrorCategory {
        USER_ERROR,
        RESOURCE_ERROR,
        INTERNAL_ERROR,
        EXTERNAL_ERROR,
        EXCHANGE_ERROR,
        TIMEOUT
    }

    private static final Map<String, ErrorCategory> PREFIX_MAP = Map.ofEntries(
            Map.entry("USER", ErrorCategory.USER_ERROR),
            Map.entry("VALIDATION", ErrorCategory.USER_ERROR),
            Map.entry("RESOURCE", ErrorCategory.RESOURCE_ERROR),
            Map.entry("MEMORY", ErrorCategory.RESOURCE_ERROR),
            Map.entry("INTERNAL", ErrorCategory.INTERNAL_ERROR),
            Map.entry("CONFIG", ErrorCategory.INTERNAL_ERROR),
            Map.entry("EXTERNAL", ErrorCategory.EXTERNAL_ERROR),
            Map.entry("METADATA", ErrorCategory.EXTERNAL_ERROR),
            Map.entry("NATIVE", ErrorCategory.EXTERNAL_ERROR),
            Map.entry("EXCHANGE", ErrorCategory.EXCHANGE_ERROR),
            Map.entry("FLIGHT", ErrorCategory.EXCHANGE_ERROR),
            Map.entry("TRANSPORT", ErrorCategory.EXCHANGE_ERROR),
            Map.entry("TENANT", ErrorCategory.USER_ERROR),
            Map.entry("TIMEOUT", ErrorCategory.TIMEOUT)
    );

    private static final Map<ErrorCategory, Boolean> RETRYABLE_MAP = Map.of(
            ErrorCategory.USER_ERROR, false,
            ErrorCategory.RESOURCE_ERROR, true,
            ErrorCategory.INTERNAL_ERROR, false,
            ErrorCategory.EXTERNAL_ERROR, true,
            ErrorCategory.EXCHANGE_ERROR, true,
            ErrorCategory.TIMEOUT, true
    );

    private static final Map<ErrorCategory, Integer> MAX_RETRIES_MAP = Map.of(
            ErrorCategory.USER_ERROR, 0,
            ErrorCategory.RESOURCE_ERROR, 1,
            ErrorCategory.INTERNAL_ERROR, 0,
            ErrorCategory.EXTERNAL_ERROR, 3,
            ErrorCategory.EXCHANGE_ERROR, 3,
            ErrorCategory.TIMEOUT, 2
    );

    public ErrorCategory classify(PangrovaErrorCode errorCode) {
        String prefix = errorCode.getCode().split("-")[0];
        return PREFIX_MAP.getOrDefault(prefix, ErrorCategory.INTERNAL_ERROR);
    }

    public boolean isRetryable(ErrorCategory category) {
        return RETRYABLE_MAP.getOrDefault(category, false);
    }

    public int maxRetries(ErrorCategory category) {
        return MAX_RETRIES_MAP.getOrDefault(category, 0);
    }
}
