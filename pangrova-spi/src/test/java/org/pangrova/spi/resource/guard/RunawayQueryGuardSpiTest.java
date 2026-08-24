package org.pangrova.spi.resource.guard;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the runaway query guard SPI contracts (Task 147, G-RES-06).
 */
class RunawayQueryGuardSpiTest {

    // --- QueryResourceSnapshot ---

    @Test
    void snapshotStoresFields() {
        var snap = new QueryResourceSnapshot("q-1", 5000, 1_000_000, 50000, 128_000_000);
        assertThat(snap.queryId()).isEqualTo("q-1");
        assertThat(snap.cpuTimeMs()).isEqualTo(5000);
        assertThat(snap.scannedBytes()).isEqualTo(1_000_000);
        assertThat(snap.scannedRows()).isEqualTo(50000);
        assertThat(snap.memoryBytes()).isEqualTo(128_000_000);
    }

    @Test
    void snapshotRejectsNullQueryId() {
        assertThatThrownBy(() -> new QueryResourceSnapshot(null, 0, 0, 0, 0))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void snapshotRejectsBlankQueryId() {
        assertThatThrownBy(() -> new QueryResourceSnapshot("  ", 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryId");
    }

    // --- GuardVerdict ---

    @Test
    void killVerdictFactory() {
        var v = GuardVerdict.kill("q-1", List.of("cpuTime: 9000 > 5000"));
        assertThat(v.queryId()).isEqualTo("q-1");
        assertThat(v.action()).isEqualTo(GuardAction.KILL);
        assertThat(v.reasons()).containsExactly("cpuTime: 9000 > 5000");
    }

    @Test
    void warnVerdictFactory() {
        var v = GuardVerdict.warn("q-2", List.of("memory: 2GB > 1GB"));
        assertThat(v.action()).isEqualTo(GuardAction.WARN);
    }

    @Test
    void verdictRejectsEmptyReasons() {
        assertThatThrownBy(() -> new GuardVerdict("q-1", GuardAction.KILL, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reasons");
    }

    @Test
    void verdictReasonsAreImmutable() {
        var mutable = new java.util.ArrayList<>(List.of("reason1"));
        var v = new GuardVerdict("q-1", GuardAction.KILL, mutable);
        mutable.add("reason2");
        assertThat(v.reasons()).hasSize(1);
    }

    // --- RunawayQueryGuard interface ---

    @Test
    void guardInterfaceCanBeImplemented() {
        RunawayQueryGuard guard = new RunawayQueryGuard() {
            private static final long MAX_CPU_MS = 5000;
            private static final long MAX_MEMORY = 1_000_000_000L;

            @Override
            public Optional<GuardVerdict> evaluate(QueryResourceSnapshot snapshot) {
                var reasons = new java.util.ArrayList<String>();
                if (snapshot.cpuTimeMs() > MAX_CPU_MS) {
                    reasons.add("cpuTimeMs: " + snapshot.cpuTimeMs() + " > " + MAX_CPU_MS);
                }
                if (snapshot.memoryBytes() > MAX_MEMORY) {
                    reasons.add("memoryBytes: " + snapshot.memoryBytes() + " > " + MAX_MEMORY);
                }
                return reasons.isEmpty()
                        ? Optional.empty()
                        : Optional.of(GuardVerdict.kill(snapshot.queryId(), reasons));
            }

            @Override
            public String name() {
                return "threshold-based";
            }
        };

        // within limits
        var ok = new QueryResourceSnapshot("q-1", 1000, 0, 0, 500_000_000);
        assertThat(guard.evaluate(ok)).isEmpty();

        // cpu exceeded
        var cpuHigh = new QueryResourceSnapshot("q-2", 9000, 0, 0, 100);
        var v = guard.evaluate(cpuHigh);
        assertThat(v).isPresent();
        assertThat(v.get().action()).isEqualTo(GuardAction.KILL);
        assertThat(v.get().reasons()).anyMatch(r -> r.contains("cpuTimeMs"));

        assertThat(guard.name()).isEqualTo("threshold-based");
    }

    @Test
    void evaluateBatchDefaultImplementation() {
        RunawayQueryGuard guard = new RunawayQueryGuard() {
            @Override
            public Optional<GuardVerdict> evaluate(QueryResourceSnapshot snapshot) {
                if (snapshot.memoryBytes() > 1_000_000) {
                    return Optional.of(GuardVerdict.kill(
                            snapshot.queryId(), List.of("memory exceeded")));
                }
                return Optional.empty();
            }

            @Override
            public String name() {
                return "test";
            }
        };

        var snapshots = List.of(
                new QueryResourceSnapshot("q-1", 0, 0, 0, 500_000),    // ok
                new QueryResourceSnapshot("q-2", 0, 0, 0, 2_000_000),  // violator
                new QueryResourceSnapshot("q-3", 0, 0, 0, 100),        // ok
                new QueryResourceSnapshot("q-4", 0, 0, 0, 5_000_000)   // violator
        );

        List<GuardVerdict> verdicts = guard.evaluateBatch(snapshots);
        assertThat(verdicts).hasSize(2);
        assertThat(verdicts.get(0).queryId()).isEqualTo("q-2");
        assertThat(verdicts.get(1).queryId()).isEqualTo("q-4");
    }
}
