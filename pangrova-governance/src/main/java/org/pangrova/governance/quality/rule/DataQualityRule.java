package org.pangrova.governance.quality.rule;

import java.time.Instant;

import org.pangrova.governance.quality.alert.Severity;

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
    Instant updatedAt
) {}
