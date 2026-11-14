package org.pangrova.worker.heartbeat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies HeartbeatSender jitter distribution stays within ±10% bounds.
 * Uses real HeartbeatSender instance with package-private nextIntervalMs().
 */
class HeartbeatJitterTest {
    private static final org.pangrova.protocol.execution.identity.WorkerExecutionIdentity HEARTBEAT_IDENTITY = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");


    private static final long BASE_INTERVAL_MS = 5_000;
    private static final int SAMPLE_COUNT = 1_000;

    private ScheduledExecutorService scheduler;
    private HeartbeatSender sender;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        HeartbeatConfig config = new HeartbeatConfig(
                "http://localhost:8080", "worker-jitter-test", BASE_INTERVAL_MS);
        sender = new HeartbeatSender(config, HeartbeatJitterTest::dummyPayload, scheduler);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void allSamplesWithinTenPercentBounds() {
        long lowerBound = BASE_INTERVAL_MS - BASE_INTERVAL_MS / 10;
        long upperBound = BASE_INTERVAL_MS + BASE_INTERVAL_MS / 10;

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            long interval = sender.nextIntervalMs();
            assertTrue(interval >= lowerBound,
                    "Interval " + interval + " below lower bound " + lowerBound);
            assertTrue(interval <= upperBound,
                    "Interval " + interval + " above upper bound " + upperBound);
        }
    }

    @Test
    void jitterProducesVariation() {
        long first = sender.nextIntervalMs();
        boolean foundDifferent = false;
        for (int i = 0; i < 100; i++) {
            if (sender.nextIntervalMs() != first) {
                foundDifferent = true;
                break;
            }
        }
        assertTrue(foundDifferent, "Jitter should produce varying intervals");
    }

    @Test
    void jitterDistributionSpansBothSidesOfBase() {
        boolean belowBase = false;
        boolean aboveBase = false;
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            long interval = sender.nextIntervalMs();
            if (interval < BASE_INTERVAL_MS) {
                belowBase = true;
            }
            if (interval > BASE_INTERVAL_MS) {
                aboveBase = true;
            }
            if (belowBase && aboveBase) {
                break;
            }
        }
        assertTrue(belowBase, "Jitter should produce values below base interval");
        assertTrue(aboveBase, "Jitter should produce values above base interval");
    }

    private static HeartbeatPayload dummyPayload() {
        return new HeartbeatPayload("worker-jitter-test", "NONE", 0.5, 0, 1024L * 1024 * 1024, HEARTBEAT_IDENTITY);
    }
}
