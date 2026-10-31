package org.pangrova.common.lifecycle;

import java.util.List;
import java.util.Objects;

/**
 * Aggregated rollback report collecting individual {@link RollbackReport} entries
 * from a bootstrap failure recovery sequence.
 *
 * <p>Provides summary counts and failure detection for structured observability
 * of rollback outcomes.</p>
 */
public final class RollbackAggregateReport {

    private final List<RollbackReport> reports;

    public RollbackAggregateReport(List<RollbackReport> reports) {
        this.reports = List.copyOf(Objects.requireNonNull(reports, "reports"));
    }

    public List<RollbackReport> reports() {
        return reports;
    }

    public int successCount() {
        return (int) reports.stream().filter(RollbackReport::success).count();
    }

    public int failureCount() {
        return (int) reports.stream().filter(r -> !r.success()).count();
    }

    public boolean hasFailures() {
        return reports.stream().anyMatch(r -> !r.success());
    }

    public List<RollbackReport> failures() {
        return reports.stream().filter(r -> !r.success()).toList();
    }

    @Override
    public String toString() {
        return "RollbackAggregateReport{total=" + reports.size()
                + ", success=" + successCount()
                + ", failures=" + failureCount() + "}";
    }
}
