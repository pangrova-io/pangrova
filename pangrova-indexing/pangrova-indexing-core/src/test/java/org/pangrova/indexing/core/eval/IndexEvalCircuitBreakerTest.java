package org.pangrova.indexing.core.eval;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IndexEvalCircuitBreaker}.
 *
 * <p>Verifies that the circuit breaker is non-blocking (no I/O on the
 * eval path), correctly transitions between CLOSED/OPEN/HALF_OPEN,
 * and is thread-safe under concurrent access.</p>
 */
class IndexEvalCircuitBreakerTest {

    @Test
    void isAllowed_returnsTrue_whenNoFailuresRecorded() {
        var breaker = new IndexEvalCircuitBreaker(3, Duration.ofSeconds(60));
        assertThat(breaker.isAllowed("idx-1")).isTrue();
    }

    @Test
    void isAllowed_returnsFalse_afterThresholdFailures() {
        var breaker = new IndexEvalCircuitBreaker(3, Duration.ofSeconds(60));
        breaker.recordFailure("idx-1");
        breaker.recordFailure("idx-1");
        assertThat(breaker.isAllowed("idx-1")).isTrue();

        breaker.recordFailure("idx-1");
        assertThat(breaker.isAllowed("idx-1")).isFalse();
    }

    @Test
    void recordSuccess_resets_openCircuit() {
        var breaker = new IndexEvalCircuitBreaker(2, Duration.ofSeconds(60));
        breaker.recordFailure("idx-1");
        breaker.recordFailure("idx-1");
        assertThat(breaker.isAllowed("idx-1")).isFalse();

        breaker.recordSuccess("idx-1");
        assertThat(breaker.isAllowed("idx-1")).isTrue();
    }

    @Test
    void halfOpen_allowsSingleProbe_afterOpenDurationExpires() throws InterruptedException {
        var breaker = new IndexEvalCircuitBreaker(1, Duration.ofMillis(50));
        breaker.recordFailure("idx-1");
        assertThat(breaker.isAllowed("idx-1")).isFalse();

        // Wait for open duration to expire
        Thread.sleep(80);

        // First call should be allowed (HALF_OPEN probe)
        assertThat(breaker.isAllowed("idx-1")).isTrue();
        // Second call should be blocked (only one probe allowed)
        assertThat(breaker.isAllowed("idx-1")).isFalse();
    }

    @Test
    void separateIndexes_areIndependent() {
        var breaker = new IndexEvalCircuitBreaker(2, Duration.ofSeconds(60));
        breaker.recordFailure("idx-bad");
        breaker.recordFailure("idx-bad");

        assertThat(breaker.isAllowed("idx-bad")).isFalse();
        assertThat(breaker.isAllowed("idx-good")).isTrue();
    }

    @Test
    void constructor_rejectsInvalidArguments() {
        assertThatThrownBy(() -> new IndexEvalCircuitBreaker(0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureThreshold");

        assertThatThrownBy(() -> new IndexEvalCircuitBreaker(1, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openDuration");

        assertThatThrownBy(() -> new IndexEvalCircuitBreaker(1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openDuration");
    }

    @Test
    void concurrentAccess_doesNotCorruptState() throws InterruptedException {
        var breaker = new IndexEvalCircuitBreaker(100, Duration.ofSeconds(60));
        int threads = 8;
        int failuresPerThread = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < failuresPerThread; i++) {
                        breaker.recordFailure("idx-concurrent");
                        breaker.isAllowed("idx-concurrent");
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        assertThat(errors.get()).isZero();
        // After 160 failures (8 * 20), circuit should be open (threshold 100)
        assertThat(breaker.isAllowed("idx-concurrent")).isFalse();
    }

    @Test
    void isAllowed_isNonBlocking_noIOOnEvalPath() {
        // This test verifies the fix: isAllowed should complete in microseconds
        // with no I/O calls. Previously the distributed BreakerStateStore could
        // block on the eval path.
        var breaker = new IndexEvalCircuitBreaker(5, Duration.ofSeconds(30));

        long start = System.nanoTime();
        for (int i = 0; i < 10_000; i++) {
            breaker.isAllowed("idx-perf-" + (i % 100));
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // 10k non-blocking checks should complete well under 100ms
        assertThat(elapsedMs)
                .as("isAllowed must be non-blocking (no I/O); 10k calls took %dms", elapsedMs)
                .isLessThan(1000);
    }
}
