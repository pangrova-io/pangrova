package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerHealthSchedulerTest {

    private WorkerManager workerManager;
    private List<String> deadNotifications;
    private ScheduledExecutorService executor;
    private WorkerHealthScheduler scheduler;

    @BeforeEach
    void setUp() {
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        deadNotifications = Collections.synchronizedList(new ArrayList<>());
        executor = Executors.newSingleThreadScheduledExecutor();
        scheduler = new WorkerHealthScheduler(workerManager, deadNotifications::add, executor);
    }

    @AfterEach
    void tearDown() {
        scheduler.stop();
        executor.shutdownNow();
    }

    @Test
    void deadWorkerTriggersCallback() {
        WorkerInfo dead = workerWithHeartbeat("w-dead", Instant.now().minusSeconds(120));
        workerManager.registerWorker(dead);

        scheduler.evaluateAll();

        assertEquals(1, deadNotifications.size());
        assertEquals("w-dead", deadNotifications.get(0));
        // Dead worker should be removed after evaluateAll
        assertTrue(workerManager.getWorker("w-dead") == null);
    }

    @Test
    void activeWorkerDoesNotTriggerCallback() {
        WorkerInfo active = workerWithHeartbeat("w-active", Instant.now());
        workerManager.registerWorker(active);

        scheduler.evaluateAll();

        assertTrue(deadNotifications.isEmpty());
        assertEquals(1, workerManager.getActiveWorkers().size());
    }

    @Test
    void stopPreventsScheduledExecution() throws Exception {
        WorkerInfo dead = workerWithHeartbeat("w-dead", Instant.now().minusSeconds(120));
        workerManager.registerWorker(dead);

        scheduler.start();
        scheduler.stop();

        // Give enough time for a potential tick to fire
        Thread.sleep(200);
        // Re-register to detect any late evaluation
        deadNotifications.clear();
        workerManager.registerWorker(dead);
        Thread.sleep(200);

        assertTrue(deadNotifications.isEmpty(), "No evaluations should run after stop()");
    }

    @Test
    void suspectWorkerDoesNotTriggerCallback() {
        // 30s ago — between SUSPECT_THRESHOLD (20s) and DEAD_THRESHOLD (60s)
        WorkerInfo suspect = workerWithHeartbeat("w-suspect", Instant.now().minusSeconds(30));
        workerManager.registerWorker(suspect);

        scheduler.evaluateAll();

        assertTrue(deadNotifications.isEmpty());
    }

    @Test
    void multipleDeadWorkersAllNotified() {
        workerManager.registerWorker(workerWithHeartbeat("w1", Instant.now().minusSeconds(120)));
        workerManager.registerWorker(workerWithHeartbeat("w2", Instant.now().minusSeconds(120)));
        workerManager.registerWorker(workerWithHeartbeat("w3", Instant.now()));

        scheduler.evaluateAll();

        assertEquals(2, deadNotifications.size());
        assertTrue(deadNotifications.contains("w1"));
        assertTrue(deadNotifications.contains("w2"));
    }

    @Test
    void evaluateAllSurvivesCallbackException() {
        // Use a callback that throws on first invocation
        List<String> captured = Collections.synchronizedList(new ArrayList<>());
        WorkerHealthScheduler throwingScheduler = new WorkerHealthScheduler(
                workerManager,
                workerId -> {
                    captured.add(workerId);
                    throw new RuntimeException("simulated callback failure");
                },
                executor);

        workerManager.registerWorker(workerWithHeartbeat("w-dead", Instant.now().minusSeconds(120)));

        // evaluateAll should not propagate the exception
        throwingScheduler.evaluateAll();

        // Callback was invoked despite the exception
        assertEquals(1, captured.size());

        // A second evaluateAll should still work (scheduler not killed)
        workerManager.registerWorker(workerWithHeartbeat("w-dead2", Instant.now().minusSeconds(120)));
        throwingScheduler.evaluateAll();
        assertEquals(2, captured.size());
    }

    private static WorkerInfo workerWithHeartbeat(String workerId, Instant lastHeartbeat) {
        return TestWorkerInfoBuilder.withHeartbeat(workerId, "localhost", 8080, 9090, lastHeartbeat);
    }
}
