package org.pangrova.worker.buffer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies that calling {@link ResultExpirationScheduler#cancelExpiration}
 * after the expiration has already fired does not throw and the query
 * remains correctly marked as expired.
 *
 * <p>This covers the race condition where a client fetches results just
 * after the expiration callback fires — cancelExpiration must be a no-op
 * in that case, not an error.</p>
 */
class ResultExpirationCancelAfterExpireTest {

    private ScheduledExecutorService scheduler;
    private CopyOnWriteArrayList<String> expiredIds;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        expiredIds = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void cancelAfterExpireIsNoOpAndQueryRemainsExpired() throws InterruptedException {
        Duration shortRetention = Duration.ofMillis(50);
        CountDownLatch callbackCompleted = new CountDownLatch(1);
        ResultExpirationScheduler expirationScheduler = new ResultExpirationScheduler(
                scheduler, shortRetention, id -> {
                    expiredIds.add(id);
                    callbackCompleted.countDown();
                });

        String queryId = "q-expire-then-cancel";
        expirationScheduler.scheduleExpiration(queryId);

        // isExpired can become visible before the callback runs on the scheduler thread.
        // Wait for the callback itself before asserting its side effect.
        assertThat(callbackCompleted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(expirationScheduler.isExpired(queryId)).isTrue();
        assertThat(expiredIds).containsExactly(queryId);

        // Now call cancelExpiration — must not throw
        assertThatCode(() -> expirationScheduler.cancelExpiration(queryId))
                .doesNotThrowAnyException();

        // Query should still be marked as expired
        assertThat(expirationScheduler.isExpired(queryId)).isTrue();
    }

    @Test
    void cancelBeforeExpirePreventsFiring() throws InterruptedException {
        Duration longRetention = Duration.ofSeconds(10);
        ResultExpirationScheduler expirationScheduler = new ResultExpirationScheduler(
                scheduler, longRetention, expiredIds::add);

        String queryId = "q-cancel-before-expire";
        expirationScheduler.scheduleExpiration(queryId);

        // Cancel immediately
        expirationScheduler.cancelExpiration(queryId);

        // Wait a bit to confirm it doesn't fire
        Thread.sleep(200);

        assertThat(expirationScheduler.isExpired(queryId)).isFalse();
        assertThat(expiredIds).isEmpty();
    }

    @Test
    void doubleScheduleIsNoOp() {
        Duration retention = Duration.ofSeconds(10);
        ResultExpirationScheduler expirationScheduler = new ResultExpirationScheduler(
                scheduler, retention, expiredIds::add);

        String queryId = "q-double-schedule";
        expirationScheduler.scheduleExpiration(queryId);
        // Second schedule should be a no-op (computeIfAbsent)
        assertThatCode(() -> expirationScheduler.scheduleExpiration(queryId))
                .doesNotThrowAnyException();
    }

}
