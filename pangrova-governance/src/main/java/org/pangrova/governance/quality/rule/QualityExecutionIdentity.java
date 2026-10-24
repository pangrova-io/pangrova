package org.pangrova.governance.quality.rule;

/** Persisted execution subject; roles and policies must be resolved afresh on every run. */
public record QualityExecutionIdentity(String principal, String tenantId) {
    public QualityExecutionIdentity {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("Quality execution principal is required");
        }
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("Quality execution tenant is required");
        }
    }
}
