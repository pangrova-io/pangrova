package org.pangrova.worker.heartbeat;

import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests heartbeat self-degradation (R7.15).
 * After N consecutive failures, worker enters degraded mode.
 */
class HeartbeatSenderDegradationTest {
    private static final org.pangrova.protocol.execution.identity.WorkerExecutionIdentity HEARTBEAT_IDENTITY = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");


    private static ScheduledExecutorService newExecutor() {
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.setRemoveOnCancelPolicy(true);
        return exec;
    }

    @Test
    void degradationThresholdIsReasonable() {
        assertThat(HeartbeatSender.DEGRADATION_THRESHOLD).isGreaterThan(5);
    }

    @Test
    void workerEntersDegradedModeAfterSustainedFailures() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicBoolean degradationTriggered = new AtomicBoolean(false);
        CountDownLatch degradationLatch = new CountDownLatch(1);
        AtomicInteger heartbeatCount = new AtomicInteger();

        // Use an unreachable host to guarantee failures
        HeartbeatConfig config = new HeartbeatConfig("http://localhost:1", "w-degrade", 20);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    heartbeatCount.incrementAndGet();
                    return new HeartbeatRequest("w-degrade", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY);
                },
                executor);

        sender.setDegradationCallback(() -> {
            degradationTriggered.set(true);
            degradationLatch.countDown();
        });

        sender.start();

        // Wait for enough failures to trigger degradation
        boolean reached = degradationLatch.await(30, TimeUnit.SECONDS);
        sender.stop();

        assertThat(reached).isTrue();
        assertThat(degradationTriggered.get()).isTrue();
        assertThat(sender.isDegraded()).isTrue();
        assertThat(heartbeatCount.get()).isGreaterThanOrEqualTo(HeartbeatSender.DEGRADATION_THRESHOLD);
    }

    @Test
    void notDegradedInitially() {
        ScheduledExecutorService executor = newExecutor();
        HeartbeatConfig config = new HeartbeatConfig("http://localhost:9999", "w-init", 1000);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> new HeartbeatRequest("w-init", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY),
                executor);

        assertThat(sender.isDegraded()).isFalse();
    }

    @Test
    void degradationCallbackNotCalledBelowThreshold() throws Exception {
        ScheduledExecutorService executor = newExecutor();
        AtomicBoolean degradationTriggered = new AtomicBoolean(false);
        AtomicInteger heartbeatCount = new AtomicInteger();

        HeartbeatConfig config = new HeartbeatConfig("http://localhost:1", "w-below", 20);
        HeartbeatSender sender = new HeartbeatSender(config,
                () -> {
                    heartbeatCount.incrementAndGet();
                    return new HeartbeatRequest("w-below", "NONE", 0.1, 0, 1024, HEARTBEAT_IDENTITY);
                },
                executor);

        sender.setDegradationCallback(() -> degradationTriggered.set(true));

        sender.start();
        // Wait for a few failures but not enough for degradation
        Thread.sleep(200);
        sender.stop();

        // With 20ms interval and 200ms wait, we get ~5-10 heartbeats
        // which is below the DEGRADATION_THRESHOLD of 10
        if (heartbeatCount.get() < HeartbeatSender.DEGRADATION_THRESHOLD) {
            assertThat(degradationTriggered.get()).isFalse();
        }
    }
}
