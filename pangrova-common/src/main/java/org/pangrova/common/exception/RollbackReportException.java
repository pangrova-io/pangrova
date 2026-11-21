package org.pangrova.common.exception;

import org.pangrova.common.lifecycle.rollback.RollbackAggregateReport;

import java.util.Objects;

/**
 * Carrier exception that attaches a {@link RollbackAggregateReport} to the
 * bootstrap failure exception via {@link Throwable#addSuppressed(Throwable)}.
 *
 * <p>This allows upstream callers to extract structured rollback details from
 * the exception chain without changing the primary exception type.</p>
 */
public final class RollbackReportException extends Exception {

    private final RollbackAggregateReport report;

    public RollbackReportException(RollbackAggregateReport report) {
        super("Rollback report: " + report);
        this.report = Objects.requireNonNull(report, "report");
    }

    public RollbackAggregateReport report() {
        return report;
    }
}
