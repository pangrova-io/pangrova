package org.pangrova.worker.resource.memory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link UnifiedMemoryLedger}.
 * Task 203: validates unified cross-domain memory accounting.
 */
class UnifiedMemoryLedgerTest {

    @Test
    void reserveAcrossDomains() {
        var ledger = new UnifiedMemoryLedger(1000);

        assertThat(ledger.reserve(MemoryDomain.JAVA_HEAP, 300)).isEqualTo(300);
        assertThat(ledger.reserve(MemoryDomain.NATIVE, 400)).isEqualTo(400);
        assertThat(ledger.reserve(MemoryDomain.OFF_HEAP, 200)).isEqualTo(200);

        assertThat(ledger.totalUsedBytes()).isEqualTo(900);
        assertThat(ledger.totalAvailableBytes()).isEqualTo(100);
    }

    @Test
    void reserveExceedingTotalBudgetReturnsCapped() {
        var ledger = new UnifiedMemoryLedger(500);

        assertThat(ledger.reserve(MemoryDomain.JAVA_HEAP, 300)).isEqualTo(300);
        // Only 200 left across all domains
        assertThat(ledger.reserve(MemoryDomain.NATIVE, 500)).isEqualTo(200);
        assertThat(ledger.totalUsedBytes()).isEqualTo(500);
    }

    @Test
    void releaseFreesCapacity() {
        var ledger = new UnifiedMemoryLedger(1000);
        ledger.reserve(MemoryDomain.NATIVE, 800);
        ledger.release(MemoryDomain.NATIVE, 500);
        assertThat(ledger.usedBytes(MemoryDomain.NATIVE)).isEqualTo(300);
        assertThat(ledger.totalAvailableBytes()).isEqualTo(700);
    }

    @Test
    void updateUsageSetsDirectly() {
        var ledger = new UnifiedMemoryLedger(1000);
        ledger.updateUsage(MemoryDomain.NATIVE, 600);
        assertThat(ledger.usedBytes(MemoryDomain.NATIVE)).isEqualTo(600);
        assertThat(ledger.totalUsedBytes()).isEqualTo(600);
    }

    @Test
    void utilisationComputation() {
        var ledger = new UnifiedMemoryLedger(1000);
        ledger.reserve(MemoryDomain.JAVA_HEAP, 500);
        assertThat(ledger.utilisation()).isEqualTo(0.5);
    }

    @Test
    void enforcementNoneWhenLow() {
        var ledger = new UnifiedMemoryLedger(1000);
        ledger.reserve(MemoryDomain.JAVA_HEAP, 100);
        var snapshots = ledger.snapshot();
        EnforcementDecision decision = ledger.enforce(snapshots);
        assertThat(decision.action()).isEqualTo(EnforcementAction.NONE);
    }

    @Test
    void enforcementSpillWhenAboveThreshold() {
        var ledger = new UnifiedMemoryLedger(1000);
        // 80% utilisation -> above spill threshold (75%) but below shrink threshold (85%)
        ledger.reserve(MemoryDomain.NATIVE, 800);
        var snapshots = ledger.snapshot();
        EnforcementDecision decision = ledger.enforce(snapshots);
        assertThat(decision.action()).isEqualTo(EnforcementAction.SPILL);
    }

    @Test
    void enforcementKillWhenCritical() {
        var ledger = new UnifiedMemoryLedger(1000);
        // 96% -> above kill threshold (95%)
        ledger.reserve(MemoryDomain.JAVA_HEAP, 960);
        var snapshots = ledger.snapshot();
        EnforcementDecision decision = ledger.enforce(snapshots);
        assertThat(decision.action()).isEqualTo(EnforcementAction.KILL_LARGEST);
    }

    @Test
    void detectDriftWhenBudgetsExceedPhysical() {
        var ledger = new UnifiedMemoryLedger(1000);
        var snapshots = Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, 0, 600),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, 0, 600));
        String drift = ledger.detectDrift(snapshots, 1000);
        assertThat(drift).contains("exceeds physical memory");
    }

    @Test
    void nameReturnsExpectedValue() {
        var ledger = new UnifiedMemoryLedger(1000);
        assertThat(ledger.name()).isEqualTo("unified-ledger");
    }

    @Test
    void constructorRejectsNonPositiveBudget() {
        assertThatThrownBy(() -> new UnifiedMemoryLedger(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentReservesNeverExceedTotalBudget() throws Exception {
        // A-MEM-01 regression: under concurrent reservations the aggregate must never
        // exceed the physical budget, and the sum of granted bytes must exactly equal
        // the ledger's reported usage. The previous check-then-act implementation
        // (read totalUsedBytes() then addAndGet()) allowed collective over-allocation.
        final long budget = 1_000_000;
        var ledger = new UnifiedMemoryLedger(budget);
        final int threads = 32;
        final int perThread = 500;
        final long chunk = 97; // non-divisor of budget to stress boundaries
        final MemoryDomain[] domains = MemoryDomain.values();
        var pool = Executors.newFixedThreadPool(threads);
        var start = new CountDownLatch(1);
        var granted = new AtomicLong(0);
        var maxObserved = new AtomicLong(0);
        try {
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < perThread; i++) {
                        long got = ledger.reserve(domains[(tid + i) % domains.length], chunk);
                        granted.addAndGet(got);
                        maxObserved.accumulateAndGet(ledger.totalUsedBytes(), Math::max);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        // Never observed over budget at any point.
        assertThat(maxObserved.get()).isLessThanOrEqualTo(budget);
        // Accounting is exact: granted total equals reported usage (nothing released).
        assertThat(ledger.totalUsedBytes()).isEqualTo(granted.get());
        assertThat(ledger.totalUsedBytes()).isLessThanOrEqualTo(budget);
    }
}
