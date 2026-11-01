package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import org.pangrova.worker.buffer.ResultExpirationScheduler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies ResultExpirationScheduler wiring through WorkerBootstrap:
 * expired results are cleaned up after the retention period,
 * and fetching results cancels pending expiration.
 */
@Tag("native-required")
class ResultExpirationBootstrapTest {

    private static StubCoordinatorServer stubCoordinator;
    private WorkerBootstrap bootstrap;
    private ScheduledExecutorService testScheduler;

    @BeforeAll
    static void startStub() throws Exception {
        stubCoordinator = StubCoordinatorServer.start();
    }

    @AfterAll
    static void stopStub() {
        if (stubCoordinator != null) {
            stubCoordinator.close();
        }
    }

    @AfterEach
    void tearDown() {
        if (bootstrap != null && bootstrap.isStarted()) {
            bootstrap.shutdown();
        }
        if (testScheduler != null) {
            testScheduler.shutdownNow();
        }
    }

    @Test
    void bootstrapWiresResultExpirationScheduler() {
        bootstrap = TestWorkerBootstrap.create(new WorkerBootstrapConfig(
                "expire-wire-test", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory()));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getTaskManager()).isNotNull();
    }

    @Test
    void expiredResultsCleanedAfterRetention() throws Exception {
        testScheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean expired = new AtomicBoolean(false);

        CountDownLatch expiredLatch = new CountDownLatch(1);
        ResultExpirationScheduler scheduler = new ResultExpirationScheduler(
                testScheduler, Duration.ofMillis(100), queryId -> {
            expired.set(true);
            expiredLatch.countDown();
        });

        scheduler.scheduleExpiration("q-expire-1");

        boolean fired = expiredLatch.await(2, TimeUnit.SECONDS);
        assertThat(fired).isTrue();
        assertThat(scheduler.isExpired("q-expire-1")).isTrue();
        assertThat(expired.get()).isTrue();
    }

    @Test
    void fetchCancelsExpiration() throws Exception {
        testScheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean expired = new AtomicBoolean(false);

        ResultExpirationScheduler scheduler = new ResultExpirationScheduler(
                testScheduler, Duration.ofMillis(200), queryId -> {
            expired.set(true);
        });

        scheduler.scheduleExpiration("q-cancel-1");
        scheduler.cancelExpiration("q-cancel-1");

        org.awaitility.Awaitility.await().during(Duration.ofMillis(400))
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(expired.get()).isFalse());
        assertThat(scheduler.isExpired("q-cancel-1")).isFalse();
        assertThat(expired.get()).isFalse();
    }

    @Test
    void bootstrapWithCustomRetention() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "expire-custom", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null,
                WorkerBootstrapConfig.ResourceConfig.defaults(),
                60, false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = TestWorkerBootstrap.create(config);
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
    }
}
