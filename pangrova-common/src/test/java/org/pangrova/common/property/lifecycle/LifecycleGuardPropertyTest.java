package org.pangrova.common.property.lifecycle;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.pangrova.common.lifecycle.LifecycleGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link LifecycleGuard}.
 *
 * <p>Verifies two universal properties:
 * <ol>
 *   <li>Any activate/deactivate sequence results in a consistent {@code isActive} state</li>
 *   <li>Concurrent {@code tryActivate()} — only one thread returns true</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 10.1-10.5</b></p>
 */
class LifecycleGuardPropertyTest {

    /**
     * Property 1: any activate/deactivate sequence results in consistent isActive state.
     *
     * <p>Given a random sequence of boolean operations (true = tryActivate, false = tryDeactivate),
     * replaying them on a fresh guard must leave {@code isActive()} matching the expected
     * state derived from a simple boolean model.
     *
     * <p>The model tracks expected state: starts false, tryActivate sets to true (if was false),
     * tryDeactivate sets to false (if was true). After replaying the full sequence,
     * the guard's isActive must match the model.
     */
    @Property(tries = 10)
    void sequenceResultsInConsistentIsActiveState(
            @ForAll @Size(min = 0, max = 50) List<Boolean> operations) {
        var guard = new LifecycleGuard();

        // Simple boolean model to track expected state
        boolean expectedActive = false;

        for (boolean activate : operations) {
            if (activate) {
                boolean result = guard.tryActivate();
                if (!expectedActive) {
                    // Transition false→true should succeed
                    assertThat(result)
                            .as("tryActivate should succeed when inactive")
                            .isTrue();
                    expectedActive = true;
                } else {
                    // Already active, should fail
                    assertThat(result)
                            .as("tryActivate should fail when already active")
                            .isFalse();
                }
            } else {
                boolean result = guard.tryDeactivate();
                if (expectedActive) {
                    // Transition true→false should succeed
                    assertThat(result)
                            .as("tryDeactivate should succeed when active")
                            .isTrue();
                    expectedActive = false;
                } else {
                    // Already inactive, should fail
                    assertThat(result)
                            .as("tryDeactivate should fail when already inactive")
                            .isFalse();
                }
            }

            assertThat(guard.isActive())
                    .as("isActive should match expected state after operation")
                    .isEqualTo(expectedActive);
        }
    }

    /**
     * Property 2: concurrent tryActivate — only one thread returns true.
     *
     * <p>Launch N threads all calling {@code tryActivate()} on a fresh guard simultaneously.
     * Exactly one thread must return true; all others must return false.
     * This validates the thread-safety guarantee of {@link LifecycleGuard}.
     */
    @Property(tries = 10)
    void concurrentTryActivateOnlyOneReturnsTrue(
            @ForAll @IntRange(min = 2, max = 32) int threadCount) throws Exception {
        var guard = new LifecycleGuard();
        var barrier = new CyclicBarrier(threadCount);
        var successCount = new AtomicInteger(0);
        var latch = new CountDownLatch(threadCount);
        var errors = new ArrayList<Throwable>();

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    if (guard.tryActivate()) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        assertThat(errors)
                .as("no thread should throw an exception")
                .isEmpty();
        assertThat(successCount.get())
                .as("exactly one thread should succeed in tryActivate with %d threads", threadCount)
                .isEqualTo(1);
        assertThat(guard.isActive())
                .as("guard should be active after one successful tryActivate")
                .isTrue();
    }
}
