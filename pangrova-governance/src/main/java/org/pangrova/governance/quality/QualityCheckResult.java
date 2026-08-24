package org.pangrova.governance.quality;

import java.time.Instant;

import org.pangrova.governance.quality.alert.Severity;

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
    String errorMessage
) {}
