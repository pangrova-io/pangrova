package org.pangrova.resource.guard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RunawayQueryGuardTest {

    private static final RunawayQueryThresholds LIMITS =
            new RunawayQueryThresholds(5000, 1_000_000, 100_000, 2_000_000);

    @Test
    void withinAllLimitsReturnsEmpty() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v =
                guard.evaluate(new QueryRuntimeMetrics("q1", 100, 100, 100, 100));
        assertThat(v).isEmpty();
    }

    @Test
    void cpuTimeBreachDetected() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v =
                guard.evaluate(new QueryRuntimeMetrics("q1", 9000, 0, 0, 0));
        assertThat(v).isPresent();
        assertThat(v.get().queryId()).isEqualTo("q1");
        assertThat(v.get().reasons()).containsExactly("cpuTimeMs: 9000 > 5000");
    }

    @Test
    void scannedBytesBreachDetected() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v =
                guard.evaluate(new QueryRuntimeMetrics("q1", 0, 2_000_000, 0, 0));
        assertThat(v).isPresent();
        assertThat(v.get().reasons()).containsExactly("scannedBytes: 2000000 > 1000000");
    }

    @Test
    void scannedRowsBreachDetected() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v =
                guard.evaluate(new QueryRuntimeMetrics("q1", 0, 0, 200_000, 0));
        assertThat(v).isPresent();
        assertThat(v.get().reasons()).containsExactly("scannedRows: 200000 > 100000");
    }

    @Test
    void memoryBreachDetected() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v =
                guard.evaluate(new QueryRuntimeMetrics("q1", 0, 0, 0, 9_000_000));
        assertThat(v).isPresent();
        assertThat(v.get().reasons()).containsExactly("memoryBytes: 9000000 > 2000000");
    }

    @Test
    void multipleBreachesAllReported() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v = guard.evaluate(
                new QueryRuntimeMetrics("q1", 9000, 2_000_000, 200_000, 9_000_000));
        assertThat(v).isPresent();
        assertThat(v.get().reasons()).containsExactly(
                "cpuTimeMs: 9000 > 5000",
                "scannedBytes: 2000000 > 1000000",
                "scannedRows: 200000 > 100000",
                "memoryBytes: 9000000 > 2000000");
    }

    @Test
    void atThresholdIsNotABreach() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        Optional<RunawayViolation> v =
                guard.evaluate(new QueryRuntimeMetrics("q1", 5000, 1_000_000, 100_000, 2_000_000));
        assertThat(v).isEmpty();
    }

    @Test
    void unlimitedEnforcesNothing() {
        RunawayQueryGuard guard = new RunawayQueryGuard(RunawayQueryThresholds.UNLIMITED);
        Optional<RunawayViolation> v = guard.evaluate(
                new QueryRuntimeMetrics("q1", Long.MAX_VALUE, Long.MAX_VALUE,
                        Long.MAX_VALUE, Long.MAX_VALUE));
        assertThat(v).isEmpty();
    }

    @Test
    void nonPositiveThresholdDisablesThatDimension() {
        // Only memory enforced; the other three disabled with <= 0.
        RunawayQueryThresholds onlyMemory = new RunawayQueryThresholds(0, -1, 0, 1000);
        RunawayQueryGuard guard = new RunawayQueryGuard(onlyMemory);
        Optional<RunawayViolation> v = guard.evaluate(
                new QueryRuntimeMetrics("q1", Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 2000));
        assertThat(v).isPresent();
        assertThat(v.get().reasons()).containsExactly("memoryBytes: 2000 > 1000");
    }

    @Test
    void selectViolatorsReturnsOnlyViolatorsInInputOrder() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        List<QueryRuntimeMetrics> batch = List.of(
                new QueryRuntimeMetrics("ok1", 1, 1, 1, 1),
                new QueryRuntimeMetrics("bad1", 9000, 0, 0, 0),
                new QueryRuntimeMetrics("ok2", 2, 2, 2, 2),
                new QueryRuntimeMetrics("bad2", 0, 0, 0, 9_000_000));
        List<RunawayViolation> violators = guard.selectViolators(batch);
        assertThat(violators).extracting(RunawayViolation::queryId)
                .containsExactly("bad1", "bad2");
    }

    @Test
    void selectViolatorsEmptyWhenAllWithinLimits() {
        RunawayQueryGuard guard = new RunawayQueryGuard(LIMITS);
        List<RunawayViolation> violators = guard.selectViolators(List.of(
                new QueryRuntimeMetrics("ok1", 1, 1, 1, 1),
                new QueryRuntimeMetrics("ok2", 2, 2, 2, 2)));
        assertThat(violators).isEmpty();
    }

    @Test
    void metricsRejectBlankQueryId() {
        assertThatThrownBy(() -> new QueryRuntimeMetrics(" ", 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void violationRejectsEmptyReasons() {
        assertThatThrownBy(() -> new RunawayViolation("q1", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void guardRejectsNullThresholds() {
        assertThatThrownBy(() -> new RunawayQueryGuard(null))
                .isInstanceOf(NullPointerException.class);
    }
}
