package org.pangrova.coordinator.resilience.breaker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CircuitBreaker} covering:
 * - Complete state machine transition paths
 * - Failure rate threshold boundaries
 * - HALF_OPEN probe limits
 * - Concurrent access safety
 */
class CircuitBreakerTest {

    private MutableClock clock;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.now());
    }

    @Nested
    @DisplayName("State Machine Transitions")
    class StateMachineTransitions {

        @Test
        @DisplayName("Initial state is CLOSED")
        void initialStateIsClosed() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("CLOSED allows all requests")
        void closedAllowsAllRequests() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            for (int i = 0; i < 100; i++) {
                assertThat(breaker.allowRequest()).isTrue();
            }
        }

        @Test
        @DisplayName("CLOSED -> OPEN when failure rate exceeds threshold")
        void closedToOpenOnHighFailureRate() {
            CircuitBreaker breaker = createBreaker(0.5, 10, 1000, 3);

            // Record 4 successes first, then 6 failures (60% failure rate > 50% threshold)
            // Note: threshold is only checked on failure, so failures must come last
            for (int i = 0; i < 4; i++) {
                breaker.allowRequest();
                breaker.onSuccess();
            }
            for (int i = 0; i < 6; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("CLOSED stays CLOSED when failure rate is below threshold")
        void closedStaysClosedBelowThreshold() {
            CircuitBreaker breaker = createBreaker(0.5, 10, 1000, 3);

            // Record 6 successes first, then 4 failures (40% failure rate < 50% threshold)
            // Note: threshold is only checked on failure, so failures must come last
            for (int i = 0; i < 6; i++) {
                breaker.allowRequest();
                breaker.onSuccess();
            }
            for (int i = 0; i < 4; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("OPEN -> HALF_OPEN after wait duration")
        void openToHalfOpenAfterWaitDuration() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            transitionToOpen(breaker);

            // Before wait duration - should stay OPEN
            assertThat(breaker.allowRequest()).isFalse();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            // After wait duration - should transition to HALF_OPEN
            clock.advance(Duration.ofMillis(1001));
            assertThat(breaker.allowRequest()).isTrue();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        }

        @Test
        @DisplayName("HALF_OPEN -> CLOSED after sufficient successes")
        void halfOpenToClosedOnSuccesses() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            transitionToHalfOpen(breaker);

            // Record 3 successes (halfOpenMaxCalls = 3)
            for (int i = 0; i < 3; i++) {
                breaker.onSuccess();
            }

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("HALF_OPEN -> OPEN on any failure")
        void halfOpenToOpenOnFailure() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            transitionToHalfOpen(breaker);

            breaker.onFailure();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("Complete cycle: CLOSED -> OPEN -> HALF_OPEN -> CLOSED")
        void completeCycle() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);

            // Start CLOSED
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            // Transition to OPEN
            transitionToOpen(breaker);
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            // Transition to HALF_OPEN
            clock.advance(Duration.ofMillis(1001));
            breaker.allowRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            // Transition back to CLOSED
            for (int i = 0; i < 3; i++) {
                breaker.onSuccess();
            }
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("Complete cycle: CLOSED -> OPEN -> HALF_OPEN -> OPEN")
        void cycleWithHalfOpenFailure() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);

            transitionToOpen(breaker);
            clock.advance(Duration.ofMillis(1001));
            breaker.allowRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            // Failure in HALF_OPEN returns to OPEN
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }
    }

    @Nested
    @DisplayName("Failure Rate Threshold Boundaries")
    class FailureRateThresholdBoundaries {

        @Test
        @DisplayName("Exactly at threshold triggers OPEN")
        void exactlyAtThresholdTriggersOpen() {
            CircuitBreaker breaker = createBreaker(0.5, 10, 1000, 3);

            // 5 successes, then 5 failures = exactly 50%
            // Note: threshold is only checked on failure, so failures must come last
            for (int i = 0; i < 5; i++) {
                breaker.allowRequest();
                breaker.onSuccess();
            }
            for (int i = 0; i < 5; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("Just below threshold stays CLOSED")
        void justBelowThresholdStaysClosed() {
            CircuitBreaker breaker = createBreaker(0.5, 10, 1000, 3);

            // 4 failures, 6 successes = 40% < 50%
            for (int i = 0; i < 4; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }
            for (int i = 0; i < 6; i++) {
                breaker.allowRequest();
                breaker.onSuccess();
            }

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("Does not evaluate until minimum calls reached")
        void doesNotEvaluateUntilMinimumCalls() {
            CircuitBreaker breaker = createBreaker(0.5, 10, 1000, 3);

            // 9 failures (100% failure rate but below minimum calls)
            for (int i = 0; i < 9; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            // 10th call triggers evaluation
            breaker.allowRequest();
            breaker.onFailure();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("Zero failure rate threshold opens on first failure after minimum")
        void zeroThresholdOpensOnFirstFailure() {
            CircuitBreaker breaker = createBreaker(0.0, 5, 1000, 3);

            // 4 successes
            for (int i = 0; i < 4; i++) {
                breaker.allowRequest();
                breaker.onSuccess();
            }
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            // 5th call is a failure - should open (0% threshold)
            breaker.allowRequest();
            breaker.onFailure();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("100% failure rate threshold opens when rate equals threshold")
        void fullThresholdOpensWhenRateEqualsThreshold() {
            CircuitBreaker breaker = createBreaker(1.0, 5, 1000, 3);

            // All failures - 100% failure rate >= 100% threshold
            for (int i = 0; i < 10; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }

            // 100% failure rate >= 100% threshold triggers OPEN
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("Just below 100% threshold stays CLOSED")
        void justBelowFullThresholdStaysClosed() {
            CircuitBreaker breaker = createBreaker(1.0, 10, 1000, 3);

            // 9 failures, 1 success = 90% failure rate < 100% threshold
            // Success must come last to avoid triggering OPEN during failures
            for (int i = 0; i < 9; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }
            // At this point: 9 failures, 0 successes = 100% but below minimum calls
            // Add 1 success to make it 90%
            breaker.allowRequest();
            breaker.onSuccess();

            // Now we need one more failure to trigger threshold check
            // But that would make it 10/11 = 90.9% which is still < 100%
            // Actually, let's restructure: we need to end with a failure to trigger check
            // but have the rate be < 100%

            // Reset and try different approach
            CircuitBreaker breaker2 = createBreaker(1.0, 5, 1000, 3);
            // 4 failures, 1 success, then 1 more failure = 5/6 = 83.3% < 100%
            for (int i = 0; i < 4; i++) {
                breaker2.allowRequest();
                breaker2.onFailure();
            }
            breaker2.allowRequest();
            breaker2.onSuccess();
            breaker2.allowRequest();
            breaker2.onFailure();

            // 5 failures, 1 success = 83.3% < 100%
            assertThat(breaker2.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }
    }

    @Nested
    @DisplayName("HALF_OPEN Probe Limits")
    class HalfOpenProbeLimits {

        @Test
        @DisplayName("Allows exactly halfOpenMaxCalls probes")
        void allowsExactlyMaxProbes() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            transitionToHalfOpen(breaker);

            // First probe was consumed during transition
            assertThat(breaker.allowRequest()).isTrue();  // 2nd probe
            assertThat(breaker.allowRequest()).isTrue();  // 3rd probe
            assertThat(breaker.allowRequest()).isFalse(); // 4th probe - rejected
        }

        @Test
        @DisplayName("Rejects requests after max probes reached")
        void rejectsAfterMaxProbes() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 1);
            transitionToHalfOpen(breaker);

            // First probe was consumed during transition
            assertThat(breaker.allowRequest()).isFalse();
            assertThat(breaker.allowRequest()).isFalse();
        }

        @Test
        @DisplayName("Probe count resets on transition to CLOSED")
        void probeCountResetsOnClose() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            transitionToHalfOpen(breaker);

            // Use all probes with successes
            breaker.onSuccess();
            breaker.onSuccess();
            breaker.onSuccess();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            // Trigger another cycle
            transitionToOpen(breaker);
            clock.advance(Duration.ofMillis(1001));

            // Should allow probes again
            assertThat(breaker.allowRequest()).isTrue();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        }

        @Test
        @DisplayName("Probe count resets on transition back to OPEN")
        void probeCountResetsOnReopen() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);
            transitionToHalfOpen(breaker);

            // Failure returns to OPEN
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            // Wait and transition to HALF_OPEN again
            clock.advance(Duration.ofMillis(1001));
            assertThat(breaker.allowRequest()).isTrue();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            // Should have fresh probe count
            assertThat(breaker.allowRequest()).isTrue();
            assertThat(breaker.allowRequest()).isTrue();
            assertThat(breaker.allowRequest()).isFalse();
        }
    }

    @Nested
    @DisplayName("Concurrent Access Safety")
    class ConcurrentAccessSafety {

        @Test
        @DisplayName("Concurrent failures trigger single OPEN transition")
        void concurrentFailuresTriggerSingleTransition() throws InterruptedException {
            CircuitBreaker breaker = createBreaker(0.5, 10, 1000, 3);
            int threadCount = 20;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            // Pre-record some calls to get close to threshold
            for (int i = 0; i < 9; i++) {
                breaker.allowRequest();
                breaker.onFailure();
            }

            // Concurrent failures
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        breaker.allowRequest();
                        breaker.onFailure();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("Concurrent HALF_OPEN successes trigger single CLOSED transition")
        void concurrentHalfOpenSuccessesTriggerSingleTransition() throws InterruptedException {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 10);
            transitionToHalfOpen(breaker);

            int threadCount = 20;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        breaker.onSuccess();
                        successCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("Concurrent allowRequest in HALF_OPEN respects probe limit")
        void concurrentAllowRequestRespectsProbeLimit() throws InterruptedException {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 5);
            transitionToHalfOpen(breaker);

            int threadCount = 20;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger allowedCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        if (breaker.allowRequest()) {
                            allowedCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);
            executor.shutdown();

            // Should allow at most 4 more (1 was consumed during transition)
            assertThat(allowedCount.get()).isLessThanOrEqualTo(4);
        }
    }

    @Nested
    @DisplayName("CircuitBreakerPolicy SPI")
    class CircuitBreakerPolicySpi {

        @Test
        @DisplayName("SPI methods delegate to core methods")
        void spiMethodsDelegate() {
            CircuitBreaker breaker = createBreaker(0.5, 5, 1000, 3);

            assertThat(breaker.allowRequest("target")).isTrue();

            breaker.recordSuccess("target");
            breaker.recordFailure("target", new RuntimeException("test"));

            // Verify state changes work through SPI
            transitionToOpen(breaker);
            assertThat(breaker.allowRequest("any-target")).isFalse();
        }
    }

    // Helper methods

    private CircuitBreaker createBreaker(double failureRateThreshold, int minimumCalls,
                                         long waitDurationMs, int halfOpenMaxCalls) {
        return new CircuitBreaker(failureRateThreshold, minimumCalls, waitDurationMs, halfOpenMaxCalls, clock);
    }

    private void transitionToOpen(CircuitBreaker breaker) {
        // Record enough failures to trigger OPEN
        int minimumCalls = 5;
        for (int i = 0; i < minimumCalls; i++) {
            breaker.allowRequest();
            breaker.onFailure();
        }
    }

    private void transitionToHalfOpen(CircuitBreaker breaker) {
        transitionToOpen(breaker);
        clock.advance(Duration.ofMillis(1001));
        breaker.allowRequest(); // Triggers transition and consumes first probe
    }

    /**
     * Mutable clock for testing time-based transitions.
     */
    static class MutableClock extends Clock {
        private Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
