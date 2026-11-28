package org.pangrova.worker.heartbeat;

import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies BUG-X02 fix: after {@code releaseTaskMemory()} completes on a
 * task-completion thread, the heartbeat payload supplier on the scheduler
 * thread immediately sees the updated {@code availableMemoryBytes}.
 *
 * <p>The {@link MemoryArbitrator} uses {@link AtomicLong} CAS for all
 * counter updates, guaranteeing cross-thread visibility without locks.</p>
 */
class HeartbeatMemoryVisibilityTest {
    private static final org.pangrova.protocol.execution.identity.WorkerExecutionIdentity HEARTBEAT_IDENTITY = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");


    private static final long TOTAL_BUDGET = 10_000_000L;
    private static final long SYSTEM_RESERVED = 0L;

    @Test
    void heartbeatReflectsMemoryReleaseImmediately() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_BUDGET, SYSTEM_RESERVED);

        // Simulate task allocation
        long taskMemory = 4_000_000L;
        long allocated = arbitrator.requestMemory("query-1", taskMemory);
        assertEquals(taskMemory, allocated);
        assertEquals(TOTAL_BUDGET - taskMemory, arbitrator.getAvailableMemory());

        // Build payload supplier that reads from the same arbitrator
        Supplier<HeartbeatRequest> payloadSupplier = () -> new HeartbeatRequest(
                "w-mem-test", "NONE", 0.0, 0, arbitrator.getAvailableMemory(), HEARTBEAT_IDENTITY);

        // Before release: payload shows reduced available memory
        HeartbeatRequest beforeRelease = payloadSupplier.get();
        assertEquals(TOTAL_BUDGET - taskMemory, beforeRelease.availableMemoryBytes());

        // Simulate task completion releasing memory
        arbitrator.release("query-1", taskMemory);

        // After release: payload immediately reflects full budget
        HeartbeatRequest afterRelease = payloadSupplier.get();
        assertEquals(TOTAL_BUDGET, afterRelease.availableMemoryBytes());
    }

    @Test
    void crossThreadMemoryVisibility() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_BUDGET, SYSTEM_RESERVED);
        long taskMemory = 5_000_000L;
        arbitrator.requestMemory("query-2", taskMemory);

        AtomicReference<Long> observedAvailable = new AtomicReference<>(0L);
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch releaseDone = new CountDownLatch(1);
        CountDownLatch readDone = new CountDownLatch(1);

        // Thread 1: task completion thread — releases memory
        Thread releaseThread = new Thread(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
                arbitrator.release("query-2", taskMemory);
                releaseDone.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, "task-completion");

        // Thread 2: heartbeat thread — reads available memory after release
        Thread heartbeatThread = new Thread(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
                releaseDone.await(5, TimeUnit.SECONDS);
                observedAvailable.set(arbitrator.getAvailableMemory());
                readDone.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, "heartbeat-reader");

        releaseThread.start();
        heartbeatThread.start();

        assertTrue(readDone.await(10, TimeUnit.SECONDS), "Heartbeat thread should complete");
        assertEquals(TOTAL_BUDGET, observedAvailable.get(),
                "Heartbeat thread must see full budget after release on another thread");

        releaseThread.join(5000);
        heartbeatThread.join(5000);
    }

    @Test
    void concurrentAllocateReleaseVisibleToHeartbeat() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_BUDGET, SYSTEM_RESERVED);
        int taskCount = 10;
        long perTaskMemory = 500_000L;
        CountDownLatch allAllocated = new CountDownLatch(taskCount);
        CountDownLatch allReleased = new CountDownLatch(taskCount);

        // Allocate memory for all tasks
        for (int i = 0; i < taskCount; i++) {
            String queryId = "query-" + i;
            long granted = arbitrator.requestMemory(queryId, perTaskMemory);
            assertEquals(perTaskMemory, granted);
            allAllocated.countDown();
        }
        allAllocated.await(5, TimeUnit.SECONDS);

        long expectedUsed = taskCount * perTaskMemory;
        assertEquals(TOTAL_BUDGET - expectedUsed, arbitrator.getAvailableMemory());

        // Release all tasks concurrently from different threads
        for (int i = 0; i < taskCount; i++) {
            final String queryId = "query-" + i;
            new Thread(() -> {
                arbitrator.release(queryId, perTaskMemory);
                allReleased.countDown();
            }, "release-" + i).start();
        }

        assertTrue(allReleased.await(10, TimeUnit.SECONDS), "All releases should complete");

        // Heartbeat payload supplier reads immediately after all releases
        long available = arbitrator.getAvailableMemory();
        assertEquals(TOTAL_BUDGET, available,
                "All released memory must be visible to heartbeat supplier");
    }

    @Test
    void heartbeatPayloadSupplierReadsAtSendTime() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_BUDGET, SYSTEM_RESERVED);
        long taskMemory = 3_000_000L;
        arbitrator.requestMemory("query-send-time", taskMemory);

        AtomicLong latestAvailable = new AtomicLong();
        ScheduledExecutorService executor = newExecutor();
        CountDownLatch heartbeatFired = new CountDownLatch(1);

        // Payload supplier captures available memory at invocation time
        Supplier<HeartbeatRequest> supplier = () -> {
            long avail = arbitrator.getAvailableMemory();
            latestAvailable.set(avail);
            heartbeatFired.countDown();
            return new HeartbeatRequest("w-send-time", "NONE", 0.0, 0, avail, HEARTBEAT_IDENTITY);
        };

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-send-time", 50);
        HeartbeatSender sender = new HeartbeatSender(config, supplier, executor);

        // Release memory before starting heartbeat
        arbitrator.release("query-send-time", taskMemory);

        sender.start();
        assertTrue(heartbeatFired.await(5, TimeUnit.SECONDS), "Heartbeat should fire");
        sender.stop();
        executor.shutdownNow();

        assertEquals(TOTAL_BUDGET, latestAvailable.get(),
                "Heartbeat payload must reflect released memory at send time");
    }

    private static ScheduledExecutorService newExecutor() {
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.setRemoveOnCancelPolicy(true);
        return exec;
    }
}
