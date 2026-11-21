package org.pangrova.governance.api.quality;

import java.util.List;

/**
 * Result of pre-commit quality validation.
 *
 * <p>Contains the overall pass/fail verdict and individual rule results.
 * A write is blocked if any rule with ERROR severity fails.</p>
 *
 * @param allowed      whether the write may proceed
 * @param ruleResults  individual results for each evaluated rule
 * @param blockReason  human-readable reason if write is blocked (null if allowed)
 */
public record WriteValidationResult(
        boolean allowed,
        List<QualityCheckResult> ruleResults,
        String blockReason
) {

    public WriteValidationResult {
        ruleResults = List.copyOf(ruleResults);
    }

    /** Creates a result indicating the write is allowed (no rules violated). */
    public static WriteValidationResult allowed(List<QualityCheckResult> results) {
        return new WriteValidationResult(true, results, null);
    }

    /** Creates a result indicating the write is blocked due to rule violations. */
    public static WriteValidationResult blocked(List<QualityCheckResult> results, String reason) {
        return new WriteValidationResult(false, results, reason);
    }
}
