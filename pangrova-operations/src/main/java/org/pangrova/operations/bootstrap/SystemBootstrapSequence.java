package org.pangrova.operations.bootstrap;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Executes bootstrap phases in strict order: INFRASTRUCTURE → COORDINATORS → WORKERS → READINESS.
 *
 * <p>Rollback behavior: when a phase fails, all previously completed phases are rolled back
 * in reverse order via {@link PhaseExecutor#rollback(BootstrapPhase)}. If a rollback step
 * itself fails, the error is logged but rollback continues for remaining phases to ensure
 * best-effort cleanup.</p>
 */
public final class SystemBootstrapSequence {

    private static final Logger LOG = LoggerFactory.getLogger(SystemBootstrapSequence.class);

    private final List<PhaseExecutor> executors;
    private final Clock clock;

    public SystemBootstrapSequence(List<PhaseExecutor> executors) {
        this(executors, Clock.systemUTC());
    }

    public SystemBootstrapSequence(List<PhaseExecutor> executors, Clock clock) {
        this.executors = List.copyOf(Objects.requireNonNull(executors, "executors"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Runs all bootstrap phases in order. On failure, rolls back completed phases in reverse.
     *
     * @throws PangrovaException if a bootstrap phase fails (after rollback attempt)
     */
    public void run() {
        List<BootstrapPhase> completedPhases = new ArrayList<>();
        for (BootstrapPhase phase : BootstrapPhase.values()) {
            try {
                executePhase(phase);
                completedPhases.add(phase);
            } catch (Exception e) {
                LOG.error("event=bootstrap_phase_failed phase={} completedPhases={}",
                        phase, completedPhases.size());
                RollbackAggregateReport rollbackReport = rollbackCompletedPhases(completedPhases);
                logAggregateReport(rollbackReport);
                PangrovaException bootstrapException = new PangrovaException(
                        PangrovaErrorCode.OPS_BOOTSTRAP_PHASE_FAILED,
                        "Bootstrap phase " + phase + " failed: " + e.getMessage(), e);
                bootstrapException.addSuppressed(
                        new RollbackReportException(rollbackReport));
                throw bootstrapException;
            }
        }
        LOG.info("event=bootstrap_sequence_completed");
    }

    private void executePhase(BootstrapPhase phase) {
        LOG.info("event=bootstrap_phase_executing phase={}", phase);
        executors.forEach(executor -> executor.execute(phase));
    }

    private RollbackAggregateReport rollbackCompletedPhases(List<BootstrapPhase> completedPhases) {
        List<RollbackReport> reports = new ArrayList<>();
        for (int i = completedPhases.size() - 1; i >= 0; i--) {
            BootstrapPhase phase = completedPhases.get(i);
            for (PhaseExecutor executor : executors) {
                String executorName = executor.getClass().getSimpleName();
                try {
                    executor.rollback(phase);
                    reports.add(RollbackReport.success(phase, executorName, clock.instant()));
                } catch (Exception rollbackError) {
                    LOG.warn("event=rollback_step_failed phase={} executor={} error={}",
                            phase, executorName, rollbackError.getMessage());
                    reports.add(RollbackReport.failure(
                            phase, executorName, rollbackError.getMessage(), clock.instant()));
                }
            }
            LOG.info("event=bootstrap_phase_rolled_back phase={}", phase);
        }
        return new RollbackAggregateReport(reports);
    }

    private void logAggregateReport(RollbackAggregateReport report) {
        if (report.hasFailures()) {
            LOG.error("event=rollback_completed_with_failures total={} success={} failures={}",
                    report.reports().size(), report.successCount(), report.failureCount());
            for (RollbackReport failure : report.failures()) {
                LOG.error("event=rollback_failure_detail phase={} executor={} error={} timestamp={}",
                        failure.phase(), failure.executorName(),
                        failure.errorMessage(), failure.timestamp());
            }
        } else {
            LOG.info("event=rollback_completed_successfully total={}", report.reports().size());
        }
    }

    /**
     * SPI for executing and rolling back a single bootstrap phase.
     */
    public interface PhaseExecutor {

        /**
         * Executes the given bootstrap phase.
         *
         * @param phase the phase to execute
         */
        void execute(BootstrapPhase phase);

        /**
         * Rolls back the given bootstrap phase. Called in reverse order when a subsequent phase fails.
         * Default implementation is a no-op for executors that do not require rollback.
         *
         * @param phase the phase to roll back
         */
        default void rollback(BootstrapPhase phase) {
            // no-op by default; override for phases that require cleanup
        }
    }
}
