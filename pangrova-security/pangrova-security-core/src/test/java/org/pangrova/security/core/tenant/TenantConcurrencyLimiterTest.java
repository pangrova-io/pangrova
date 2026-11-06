package org.pangrova.security.core.tenant;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TenantConcurrencyLimiterTest {

    @Test
    void reservesUpToLimitThenRejects() {
        TenantConcurrencyLimiter limiter = new TenantConcurrencyLimiter();
        assertThat(limiter.tryAcquire("t1", 2)).isTrue();
        assertThat(limiter.tryAcquire("t1", 2)).isTrue();
        assertThat(limiter.tryAcquire("t1", 2)).isFalse();
        assertThat(limiter.currentInFlight("t1")).isEqualTo(2);
    }

    @Test
    void releaseFreesSlot() {
        TenantConcurrencyLimiter limiter = new TenantConcurrencyLimiter();
        limiter.tryAcquire("t1", 1);
        assertThat(limiter.tryAcquire("t1", 1)).isFalse();
        limiter.release("t1");
        assertThat(limiter.tryAcquire("t1", 1)).isTrue();
    }

    @Test
    void zeroLimitPermitsNothing() {
        TenantConcurrencyLimiter limiter = new TenantConcurrencyLimiter();
        assertThat(limiter.tryAcquire("t1", 0)).isFalse();
    }

    @Test
    void tenantsAreIsolated() {
        TenantConcurrencyLimiter limiter = new TenantConcurrencyLimiter();
        assertThat(limiter.tryAcquire("a", 1)).isTrue();
        assertThat(limiter.tryAcquire("b", 1)).isTrue();
        assertThat(limiter.tryAcquire("a", 1)).isFalse();
    }

    @Test
    void releaseFloorsAtZero() {
        TenantConcurrencyLimiter limiter = new TenantConcurrencyLimiter();
        limiter.release("t1");
        limiter.release("t1");
        assertThat(limiter.currentInFlight("t1")).isEqualTo(0);
    }

    @Test
    void concurrentAcquire_neverExceedsLimit() throws InterruptedException {
        TenantConcurrencyLimiter limiter = new TenantConcurrencyLimiter();
        int max = 50;
        int threads = 32;
        int attemptsPerThread = 100;
        AtomicInteger granted = new AtomicInteger();
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                for (int i = 0; i < attemptsPerThread; i++) {
                    if (limiter.tryAcquire("shared", max)) {
                        granted.incrementAndGet();
                    }
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }
        // No releases happened, so total grants must never exceed the hard cap.
        assertThat(granted.get()).isEqualTo(max);
        assertThat(limiter.currentInFlight("shared")).isEqualTo(max);
    }
}
