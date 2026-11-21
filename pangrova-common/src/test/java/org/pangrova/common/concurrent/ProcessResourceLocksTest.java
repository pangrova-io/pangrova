package org.pangrova.common.concurrent;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

class ProcessResourceLocksTest {
    @Test
    void oppositeMultiKeyOrdersNeverDeadlockOrEnterTheSameResourceTogether() throws Exception {
        var inside = new AtomicBoolean();
        var visits = new AtomicInteger();
        var start = new CountDownLatch(1);
        var threads = Executors.newFixedThreadPool(2);
        try {
            var a = threads.submit(() -> visit(List.of("resource-a", "resource-b"), start, inside, visits));
            var b = threads.submit(() -> visit(List.of("resource-b", "resource-a"), start, inside, visits));
            start.countDown();
            a.get(5, TimeUnit.SECONDS); b.get(5, TimeUnit.SECONDS);
            assertThat(visits).hasValue(1_000);
        } finally { start.countDown(); threads.shutdownNow(); assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test
    void interruptedAdmissionReleasesAnyEarlierMutexAndDoesNotRetireTheOwner() throws Exception {
        var entered = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        var unexpected = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var thread = new Thread(() -> {
            entered.countDown();
            try (var lease = ProcessResourceLocks.acquire(List.of("resource-b", "resource-a"))) {
                unexpected.set(new AssertionError("Interrupted waiter was admitted"));
            } catch (InterruptedException expected) { interrupted.set(true); }
            catch (Throwable failure) { unexpected.set(failure); }
        });
        try (var owner = ProcessResourceLocks.acquire(List.of("resource-a"))) {
            thread.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            thread.interrupt(); thread.join(5_000);
            assertThat(thread.isAlive()).isFalse();
            assertThat(interrupted).isTrue();
            assertThat(unexpected.get()).isNull();
            assertThatThrownBy(() -> ProcessResourceLocks.acquire(List.of("resource-a")))
                    .isInstanceOf(IllegalStateException.class);
        } finally { if (thread.isAlive()) { thread.interrupt(); thread.join(5_000); } }
        try (var next = ProcessResourceLocks.acquire(List.of("resource-a", "resource-b"))) { }
    }

    @Test
    void onlyTheAcquiringThreadCanCloseAndTheOwnerCanCloseTwice() throws Exception {
        var lease = ProcessResourceLocks.acquire(List.of("resource-close"));
        var threads = Executors.newSingleThreadExecutor();
        try {
            threads.submit(() -> assertThatThrownBy(lease::close).isInstanceOf(IllegalStateException.class))
                    .get(5, TimeUnit.SECONDS);
            lease.close(); lease.close();
            try (var next = ProcessResourceLocks.acquire(List.of("resource-close"))) { }
        } finally { lease.close(); threads.shutdownNow(); assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test
    void rejectedNestedAdmissionDoesNotReleaseTheOuterLease() throws Exception {
        try (var outer = ProcessResourceLocks.acquire(List.of("resource-nested"))) {
            assertThatThrownBy(() -> ProcessResourceLocks.acquire(List.of("resource-other", "resource-nested")))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> ProcessResourceLocks.acquire(List.of("resource-nested")))
                    .isInstanceOf(IllegalStateException.class);
        }
        try (var next = ProcessResourceLocks.acquire(List.of("resource-nested", "resource-other"))) { }
    }

    private static void visit(List<String> keys, CountDownLatch start, AtomicBoolean inside, AtomicInteger visits) {
        try {
            start.await();
            for (int i = 0; i < 500; i++) {
                try (var lease = ProcessResourceLocks.acquire(keys)) {
                    assertThat(inside.compareAndSet(false, true)).isTrue();
                    visits.incrementAndGet();
                    inside.set(false);
                }
            }
        } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
}
