package org.pangrova.security.tenant.quota;

import java.util.List;

/**
 * Result of a pre-submit quota evaluation: pass or fail with violation details.
 */
public record QuotaCheckResult(boolean passed, List<String> violations) {

    /**
     * Defensive copy of violations to ensure immutability.
     */
    public QuotaCheckResult {
        violations = violations != null ? List.copyOf(violations) : List.of();
    }

    public static QuotaCheckResult pass() {
        return new QuotaCheckResult(true, List.of());
    }

    public static QuotaCheckResult fail(List<String> violations) {
        return new QuotaCheckResult(false, violations);
    }
}
