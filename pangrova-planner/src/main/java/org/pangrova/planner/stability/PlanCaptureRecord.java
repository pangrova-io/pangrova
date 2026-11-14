package org.pangrova.planner.stability;

import java.time.Instant;
import java.util.Objects;

/**
 * A single observation of an optimized plan choice for a query: the plan hash
 * (canonical SHA-256 of the optimized RelNode tree, see
 * {@link PlanHasher#planHash(org.apache.calcite.rel.RelNode)}) and the wall-clock
 * timestamp at which the planner produced it.
 *
 * <p>Records are emitted by {@link PlanCaptureRegistry#record} and surfaced via
 * {@link PlanCaptureRegistry#recentPlans(String)} for analysis/diagnostics.</p>
 */
public record PlanCaptureRecord(String planHash, Instant capturedAt) {

    public PlanCaptureRecord {
        Objects.requireNonNull(planHash, "planHash");
        Objects.requireNonNull(capturedAt, "capturedAt");
    }
}
