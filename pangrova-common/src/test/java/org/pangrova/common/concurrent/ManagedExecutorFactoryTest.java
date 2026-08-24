package org.pangrova.common.concurrent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ManagedExecutorFactory}.
 * Validates thread naming, daemon status, and removeOnCancelPolicy configuration.
 */
class ManagedExecutorFactoryTest {

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }

    @Test
    void newSingleThreadExecutor_createsNamedDaemonThread() throws Exception {
        executor = ManagedExecutorFactory.newSingleThreadExecutor("test-single");
        
        var threadRef = new Thread[1];
        executor.submit(() -> threadRef[0] = Thread.currentThread()).get(1, TimeUnit.SECONDS);
        
        assertNotNull(threadRef[0]);
        assertTrue(threadRef[0].getName().startsWith("test-single-"));
        assertTrue(threadRef[0].isDaemon(), "Thread should be daemon");
    }

    @Test
    void newFixedThreadPool_createsNamedDaemonThreads() throws Exception {
        executor = ManagedExecutorFactory.newFixedThreadPool("test-fixed", 2);
        
        var threadRef = new Thread[1];
        executor.submit(() -> threadRef[0] = Thread.currentThread()).get(1, TimeUnit.SECONDS);
        
        assertNotNull(threadRef[0]);
        assertTrue(threadRef[0].getName().startsWith("test-fixed-"));
        assertTrue(threadRef[0].isDaemon(), "Thread should be daemon");
    }

    @Test
    void newSingleThreadScheduledExecutor_createsNamedDaemonThread() throws Exception {
        scheduledExecutor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("test-scheduled");
        
        var threadRef = new Thread[1];
        scheduledExecutor.submit(() -> threadRef[0] = Thread.currentThread()).get(1, TimeUnit.SECONDS);
        
        assertNotNull(threadRef[0]);
        assertTrue(threadRef[0].getName().startsWith("test-scheduled-"));
        assertTrue(threadRef[0].isDaemon(), "Thread should be daemon");
    }

    @Test
    void newSingleThreadScheduledExecutor_hasRemoveOnCancelPolicyEnabled() {
        scheduledExecutor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("test-cancel-policy");
        
        assertInstanceOf(ScheduledThreadPoolExecutor.class, scheduledExecutor);
        var stpe = (ScheduledThreadPoolExecutor) scheduledExecutor;
        assertTrue(stpe.getRemoveOnCancelPolicy(), 
            "removeOnCancelPolicy should be enabled to prevent memory leaks from cancelled tasks");
    }

    @Test
    void cancelledScheduledTask_removedFromQueue() throws Exception {
        scheduledExecutor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("cancel-test");
        var stpe = (ScheduledThreadPoolExecutor) scheduledExecutor;

        // Schedule tasks far in the future so they stay queued
        var futures = new java.util.concurrent.ScheduledFuture<?>[5];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = stpe.schedule(() -> {}, 1, TimeUnit.HOURS);
        }
        assertEquals(5, stpe.getQueue().size(), "All tasks should be queued");

        // Cancel all tasks
        for (var f : futures) {
            f.cancel(false);
        }

        // With removeOnCancelPolicy=true, cancelled tasks are removed from the queue
        assertEquals(0, stpe.getQueue().size(),
                "Cancelled tasks should be removed from the queue (removeOnCancelPolicy=true)");
    }

    @Test
    void newScheduledThreadPool_hasRemoveOnCancelPolicyEnabled() {
        scheduledExecutor = ManagedExecutorFactory.newScheduledThreadPool("pool-cancel-policy", 2);

        assertInstanceOf(ScheduledThreadPoolExecutor.class, scheduledExecutor);
        var stpe = (ScheduledThreadPoolExecutor) scheduledExecutor;
        assertTrue(stpe.getRemoveOnCancelPolicy(),
            "removeOnCancelPolicy should be enabled to prevent memory leaks from cancelled tasks");
    }

    @Test
    void newScheduledThreadPool_createsNamedDaemonThreads() throws Exception {
        scheduledExecutor = ManagedExecutorFactory.newScheduledThreadPool("pool-daemon", 2);

        var threadRef = new Thread[1];
        scheduledExecutor.submit(() -> threadRef[0] = Thread.currentThread()).get(1, TimeUnit.SECONDS);

        assertNotNull(threadRef[0]);
        assertTrue(threadRef[0].getName().startsWith("pool-daemon-"),
            "Thread name should start with the given prefix");
        assertTrue(threadRef[0].isDaemon(), "Thread should be daemon");
    }

    @Test
    void threadNaming_incrementsCounter() throws Exception {
        executor = ManagedExecutorFactory.newFixedThreadPool("counter-test", 3);
        
        var names = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var latch = new java.util.concurrent.CountDownLatch(3);
        
        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                names.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }
        
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(3, names.size());
        
        // Verify unique thread names with incrementing counter
        var sortedNames = names.stream().sorted().toList();
        assertTrue(sortedNames.get(0).endsWith("-0"));
        assertTrue(sortedNames.get(1).endsWith("-1"));
        assertTrue(sortedNames.get(2).endsWith("-2"));
    }
}
