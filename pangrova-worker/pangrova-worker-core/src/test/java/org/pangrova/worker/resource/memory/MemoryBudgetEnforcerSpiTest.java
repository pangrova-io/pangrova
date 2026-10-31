package org.pangrova.worker.resource.memory;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the memory budget enforcement SPI contracts (Task 148, G-RES-07).
 */
class MemoryBudgetEnforcerSpiTest {

    // --- MemoryDomainSnapshot ---

    @Test
    void domainSnapshotStoresFields() {
        var snap = new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 500_000_000, 1_000_000_000);
        assertThat(snap.domain()).isEqualTo(MemoryDomain.JAVA_HEAP);
        assertThat(snap.usedBytes()).isEqualTo(500_000_000);
        assertThat(snap.budgetBytes()).isEqualTo(1_000_000_000);
    }

    @Test
    void domainSnapshotUtilisation() {
        var snap = new MemoryDomainSnapshot(MemoryDomain.NATIVE, 750, 1000);
        assertThat(snap.utilisation()).isEqualTo(0.75);
    }

    @Test
    void domainSnapshotOverCommitted() {
        var over = new MemoryDomainSnapshot(MemoryDomain.NATIVE, 1500, 1000);
        assertThat(over.isOverCommitted()).isTrue();
        assertThat(over.utilisation()).isGreaterThan(1.0);

        var under = new MemoryDomainSnapshot(MemoryDomain.NATIVE, 500, 1000);
        assertThat(under.isOverCommitted()).isFalse();
    }

    @Test
    void domainSnapshotRejectsNegativeUsed() {
        assertThatThrownBy(() -> new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, -1, 1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usedBytes");
    }

    @Test
    void domainSnapshotRejectsZeroBudget() {
        assertThatThrownBy(() -> new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("budgetBytes");
    }

    @Test
    void domainSnapshotRejectsNullDomain() {
        assertThatThrownBy(() -> new MemoryDomainSnapshot(null, 0, 1000))
                .isInstanceOf(NullPointerException.class);
    }

    // --- EnforcementDecision ---

    @Test
    void noneDecision() {
        EnforcementDecision d = EnforcementDecision.none();
        assertThat(d.action()).isEqualTo(EnforcementAction.NONE);
        assertThat(d.targetDomain()).isNull();
    }

    @Test
    void spillDecision() {
        EnforcementDecision d = EnforcementDecision.spill(MemoryDomain.JAVA_HEAP, "80% used");
        assertThat(d.action()).isEqualTo(EnforcementAction.SPILL);
        assertThat(d.targetDomain()).isEqualTo(MemoryDomain.JAVA_HEAP);
        assertThat(d.reason()).isEqualTo("80% used");
    }

    @Test
    void shrinkNativeDecision() {
        EnforcementDecision d = EnforcementDecision.shrinkNative("global pressure");
        assertThat(d.action()).isEqualTo(EnforcementAction.SHRINK_NATIVE);
        assertThat(d.targetDomain()).isEqualTo(MemoryDomain.NATIVE);
    }

    @Test
    void killLargestDecision() {
        EnforcementDecision d = EnforcementDecision.killLargest(MemoryDomain.NATIVE, "OOM imminent");
        assertThat(d.action()).isEqualTo(EnforcementAction.KILL_LARGEST);
    }

    @Test
    void decisionRejectsNullAction() {
        assertThatThrownBy(() -> new EnforcementDecision(null, null, "r"))
                .isInstanceOf(NullPointerException.class);
    }

    // --- MemoryBudgetEnforcer interface ---

    @Test
    void enforcerInterfaceCanBeImplemented() {
        MemoryBudgetEnforcer enforcer = new MemoryBudgetEnforcer() {
            private static final double SPILL_THRESHOLD = 0.80;
            private static final double KILL_THRESHOLD = 0.95;

            @Override
            public EnforcementDecision enforce(Map<MemoryDomain, MemoryDomainSnapshot> snapshots) {
                double merged = mergedUtilisation(snapshots);
                if (merged >= KILL_THRESHOLD) {
                    return EnforcementDecision.killLargest(MemoryDomain.NATIVE, "merged=" + merged);
                }
                if (merged >= SPILL_THRESHOLD) {
                    return EnforcementDecision.spill(MemoryDomain.JAVA_HEAP, "merged=" + merged);
                }
                return EnforcementDecision.none();
            }

            @Override
            public double mergedUtilisation(Map<MemoryDomain, MemoryDomainSnapshot> snapshots) {
                long totalUsed = 0;
                long totalBudget = 0;
                for (var snap : snapshots.values()) {
                    totalUsed += snap.usedBytes();
                    totalBudget += snap.budgetBytes();
                }
                return totalBudget == 0 ? 0.0 : (double) totalUsed / totalBudget;
            }

            @Override
            public String detectDrift(
                    Map<MemoryDomain, MemoryDomainSnapshot> snapshots,
                    long physicalMemoryBytes) {
                long totalBudget = snapshots.values().stream()
                        .mapToLong(MemoryDomainSnapshot::budgetBytes).sum();
                if (totalBudget > physicalMemoryBytes) {
                    return "budget sum " + totalBudget + " exceeds physical " + physicalMemoryBytes;
                }
                return "";
            }

            @Override
            public String name() {
                return "threshold-based";
            }
        };

        // low usage -> NONE
        var low = Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 200, 1000),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, 300, 1000));
        assertThat(enforcer.enforce(low).action()).isEqualTo(EnforcementAction.NONE);
        assertThat(enforcer.mergedUtilisation(low)).isEqualTo(0.25);

        // high usage -> SPILL
        var high = Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 900, 1000),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, 800, 1000));
        assertThat(enforcer.enforce(high).action()).isEqualTo(EnforcementAction.SPILL);

        // critical usage -> KILL
        var critical = Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 950, 1000),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, 980, 1000));
        assertThat(enforcer.enforce(critical).action()).isEqualTo(EnforcementAction.KILL_LARGEST);

        // drift detection
        var driftSnaps = Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 0, 8_000_000_000L),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, 0, 8_000_000_000L));
        String drift = enforcer.detectDrift(driftSnaps, 12_000_000_000L);
        assertThat(drift).contains("exceeds physical");

        // no drift
        var noDriftSnaps = Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 0, 4_000_000_000L),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, 0, 4_000_000_000L));
        assertThat(enforcer.detectDrift(noDriftSnaps, 16_000_000_000L)).isEmpty();

        assertThat(enforcer.name()).isEqualTo("threshold-based");
    }
}
