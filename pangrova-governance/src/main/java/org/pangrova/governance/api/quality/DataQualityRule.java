package org.pangrova.governance.api.quality;

import java.time.Instant;

/**
 * Data quality rule definition.
 */
public record DataQualityRule(
    long ruleId,
    String ruleName,
    String catalogName,
    String schemaName,
    String tableName,
    String checkExpression,
    Severity severity,
    String message,
    boolean enabled,
    String schedule,
    Instant createdAt,
    Instant updatedAt,
    QualityExecutionIdentity executionIdentity,
    java.util.UUID tableId
) {
    public DataQualityRule {
        if (enabled && executionIdentity == null) {
            throw new IllegalArgumentException("Enabled quality rules require an execution identity");
        }
    }
}
