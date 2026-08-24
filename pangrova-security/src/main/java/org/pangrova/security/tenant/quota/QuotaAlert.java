package org.pangrova.security.tenant.quota;

/**
 * Alert raised when a tenant's usage of a specific metric approaches or exceeds thresholds.
 */
public record QuotaAlert(
        String tenantId,
        String metric,
        AlertSeverity severity,
        double usageRatio
) {}
