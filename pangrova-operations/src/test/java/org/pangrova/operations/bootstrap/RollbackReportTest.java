package org.pangrova.operations.bootstrap;

import org.pangrova.common.exception.PangrovaException;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for structured rollback reporting in SystemBootstrapSequence.
 *
 * <p>Validates: Requirements 1.59, 2.59</p>
 */
class RollbackReportTest {

    private static final Instant FIXED_TIME = Instant.parse("2025-01-15T10:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_TIME, ZoneOffset.UTC);

    @Test
    void rollbackFailureProducesStructuredReportWithPhaseExecutorErrorDetails() {
        // Executor that fails on INFRASTRUCTURE phase during execute,
        // and fails rollback on INFRASTRUCTURE
        List<String> rollbackLog = new ArrayList<>();
        SystemBootstrapSequence.PhaseExecutor failingExecutor = new SystemBootstrapSequence.PhaseExecutor() {
            @Override
            public void execute(BootstrapPhase phase) {
                if (phase == BootstrapPhase.COORDINATORS) {
                    throw new RuntimeException("coordinator init failed");
                }
            }

            @Override
            public void rollback(BootstrapPhase phase) {
                rollbackLog.add("rollback:" + phase);
                if (phase == BootstrapPhase.INFRASTRUCTURE) {
                    throw new RuntimeException("cannot release port 8080");
                }
            }
        };

        SystemBootstrapSequence sequence = new SystemBootstrapSequence(
                List.of(failingExecutor), FIXED_CLOCK);

        assertThatThrownBy(sequence::run)
                .isInstanceOf(PangrovaException.class)
                .satisfies(thrown -> {
                    Throwable[] suppressed = thrown.getSuppressed();
                    assertThat(suppressed).hasSize(1);
                    assertThat(suppressed[0]).isInstanceOf(RollbackReportException.class);

                    RollbackAggregateReport report =
                            ((RollbackReportException) suppressed[0]).report();

                    // INFRASTRUCTURE was the only completed phase before COORDINATORS failed
                    assertThat(report.reports()).hasSize(1);
                    assertThat(report.hasFailures()).isTrue();
                    assertThat(report.failureCount()).isEqualTo(1);
                    assertThat(report.successCount()).isEqualTo(0);

                    RollbackReport failure = report.failures().get(0);
                    assertThat(failure.phase()).isEqualTo(BootstrapPhase.INFRASTRUCTURE);
                    assertThat(failure.success()).isFalse();
                    assertThat(failure.errorMessage()).isEqualTo("cannot release port 8080");
                    assertThat(failure.timestamp()).isEqualTo(FIXED_TIME);
                });

        // Verify rollback was still attempted
        assertThat(rollbackLog).containsExactly("rollback:INFRASTRUCTURE");
    }

    @Test
    void rollbackSuccessProducesAllSuccessReport() {
        List<String> rollbackLog = new ArrayList<>();
        SystemBootstrapSequence.PhaseExecutor executor = new SystemBootstrapSequence.PhaseExecutor() {
            @Override
            public void execute(BootstrapPhase phase) {
                if (phase == BootstrapPhase.WORKERS) {
                    throw new RuntimeException("worker init failed");
                }
            }

            @Override
            public void rollback(BootstrapPhase phase) {
                rollbackLog.add("rollback:" + phase);
                // all rollbacks succeed
            }
        };

        SystemBootstrapSequence sequence = new SystemBootstrapSequence(
                List.of(executor), FIXED_CLOCK);

        assertThatThrownBy(sequence::run)
                .isInstanceOf(PangrovaException.class)
                .satisfies(thrown -> {
                    RollbackAggregateReport report = extractReport(thrown);

                    // INFRASTRUCTURE and COORDINATORS completed before WORKERS failed
                    assertThat(report.reports()).hasSize(2);
                    assertThat(report.hasFailures()).isFalse();
                    assertThat(report.successCount()).isEqualTo(2);
                    assertThat(report.failureCount()).isEqualTo(0);

                    // Verify reverse order: COORDINATORS first, then INFRASTRUCTURE
                    assertThat(report.reports().get(0).phase())
                            .isEqualTo(BootstrapPhase.COORDINATORS);
                    assertThat(report.reports().get(1).phase())
                            .isEqualTo(BootstrapPhase.INFRASTRUCTURE);
                });

        assertThat(rollbackLog).containsExactly(
                "rollback:COORDINATORS", "rollback:INFRASTRUCTURE");
    }

    @Test
    void rollbackContinuesAfterIndividualFailures() {
        List<String> rollbackLog = new ArrayList<>();
        SystemBootstrapSequence.PhaseExecutor executor = new SystemBootstrapSequence.PhaseExecutor() {
            @Override
            public void execute(BootstrapPhase phase) {
                if (phase == BootstrapPhase.READINESS) {
                    throw new RuntimeException("readiness check failed");
                }
            }

            @Override
            public void rollback(BootstrapPhase phase) {
                rollbackLog.add("rollback:" + phase);
                if (phase == BootstrapPhase.COORDINATORS) {
                    throw new RuntimeException("coordinator rollback failed");
                }
                // WORKERS and INFRASTRUCTURE rollback succeed
            }
        };

        SystemBootstrapSequence sequence = new SystemBootstrapSequence(
                List.of(executor), FIXED_CLOCK);

        assertThatThrownBy(sequence::run)
                .isInstanceOf(PangrovaException.class)
                .satisfies(thrown -> {
                    RollbackAggregateReport report = extractReport(thrown);

                    // 3 phases completed: INFRASTRUCTURE, COORDINATORS, WORKERS
                    assertThat(report.reports()).hasSize(3);
                    assertThat(report.successCount()).isEqualTo(2);
                    assertThat(report.failureCount()).isEqualTo(1);

                    // Verify the failure is for COORDINATORS
                    RollbackReport failure = report.failures().get(0);
                    assertThat(failure.phase()).isEqualTo(BootstrapPhase.COORDINATORS);
                    assertThat(failure.errorMessage()).isEqualTo("coordinator rollback failed");
                });

        // All 3 phases were attempted in reverse order despite COORDINATORS failure
        assertThat(rollbackLog).containsExactly(
                "rollback:WORKERS",
                "rollback:COORDINATORS",
                "rollback:INFRASTRUCTURE");
    }

    @Test
    void multipleExecutorsProduceReportsPerExecutor() {
        SystemBootstrapSequence.PhaseExecutor executorA = new SystemBootstrapSequence.PhaseExecutor() {
            @Override
            public void execute(BootstrapPhase phase) {
                if (phase == BootstrapPhase.COORDINATORS) {
                    throw new RuntimeException("exec A failed");
                }
            }

            @Override
            public void rollback(BootstrapPhase phase) {
                // succeeds
            }
        };

        SystemBootstrapSequence.PhaseExecutor executorB = new SystemBootstrapSequence.PhaseExecutor() {
            @Override
            public void execute(BootstrapPhase phase) {
                // succeeds
            }

            @Override
            public void rollback(BootstrapPhase phase) {
                throw new RuntimeException("exec B rollback failed");
            }
        };

        SystemBootstrapSequence sequence = new SystemBootstrapSequence(
                List.of(executorA, executorB), FIXED_CLOCK);

        assertThatThrownBy(sequence::run)
                .isInstanceOf(PangrovaException.class)
                .satisfies(thrown -> {
                    RollbackAggregateReport report = extractReport(thrown);

                    // INFRASTRUCTURE completed, 2 executors each produce a report
                    assertThat(report.reports()).hasSize(2);
                    assertThat(report.successCount()).isEqualTo(1);
                    assertThat(report.failureCount()).isEqualTo(1);
                });
    }

    @Test
    void rollbackReportDtoFieldsAreCorrect() {
        RollbackReport success = RollbackReport.success(
                BootstrapPhase.WORKERS, "TestExecutor", FIXED_TIME);
        assertThat(success.phase()).isEqualTo(BootstrapPhase.WORKERS);
        assertThat(success.executorName()).isEqualTo("TestExecutor");
        assertThat(success.success()).isTrue();
        assertThat(success.errorMessage()).isNull();
        assertThat(success.timestamp()).isEqualTo(FIXED_TIME);

        RollbackReport failure = RollbackReport.failure(
                BootstrapPhase.INFRASTRUCTURE, "FailExecutor", "disk full", FIXED_TIME);
        assertThat(failure.phase()).isEqualTo(BootstrapPhase.INFRASTRUCTURE);
        assertThat(failure.executorName()).isEqualTo("FailExecutor");
        assertThat(failure.success()).isFalse();
        assertThat(failure.errorMessage()).isEqualTo("disk full");
        assertThat(failure.timestamp()).isEqualTo(FIXED_TIME);
    }

    @Test
    void aggregateReportIsImmutable() {
        List<RollbackReport> mutableList = new ArrayList<>();
        mutableList.add(RollbackReport.success(
                BootstrapPhase.INFRASTRUCTURE, "Exec", FIXED_TIME));

        RollbackAggregateReport report = new RollbackAggregateReport(mutableList);
        mutableList.add(RollbackReport.success(
                BootstrapPhase.WORKERS, "Exec2", FIXED_TIME));

        // Aggregate report should not be affected by external mutation
        assertThat(report.reports()).hasSize(1);
    }

    private static RollbackAggregateReport extractReport(Throwable thrown) {
        Throwable[] suppressed = thrown.getSuppressed();
        assertThat(suppressed).hasSize(1);
        assertThat(suppressed[0]).isInstanceOf(RollbackReportException.class);
        return ((RollbackReportException) suppressed[0]).report();
    }
}
