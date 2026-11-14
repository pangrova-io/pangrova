package org.pangrova.governance.api.quality;

import org.pangrova.governance.api.enums.Severity;

import java.time.Instant;

/**
 * Result of a data quality check execution.
 */
public record QualityCheckResult(
    long resultId,
    long ruleId,
    String ruleName,
    String tableName,
    Severity severity,
    Instant checkTime,
    boolean passed,
    long totalRows,
    long failedRows,
    long executionTimeMs,
    String errorMessage,
    java.util.UUID tableId
) {}
