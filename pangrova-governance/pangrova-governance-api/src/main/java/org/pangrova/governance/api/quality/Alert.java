package org.pangrova.governance.api.quality;

import java.time.Instant;

/**
 * Alert generated from a failed quality check.
 */
public record Alert(
    String ruleName,
    String tableName,
    Severity severity,
    String message,
    long failedRows,
    long totalRows,
    Instant checkTime
) {}
