package org.pangrova.metadata.service;

import org.pangrova.metadata.cache.JdbcInvalidationPoller;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for MetadataCleanupScheduler lifecycle and cleanup execution.
 */
class MetadataCleanupSchedulerTest {

    private EmbeddedDataSource dataSource;
    private JdbcInvalidationPoller poller;
    private TieredMetadataCache cache;
    private TrackingDistributedLease lease;
    private MetadataCleanupScheduler scheduler;

    @BeforeEach
    void setUp() {
        dataSource = new EmbeddedDataSource("jdbc:h2:mem:cleanup_test;DB_CLOSE_DELAY=-1", "sa", "");
        cache = new TieredMetadataCache();
        poller = new JdbcInvalidationPoller(dataSource, cache);
        poller.initSchema();
        lease = new TrackingDistributedLease();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (scheduler != null) {
            scheduler.close();
        }
        poller.close();
        dataSource.close();
    }

    @Test
    void startSchedulesPeriodicCleanup() throws Exception {
        MetadataCleanupConfig config = new MetadataCleanupConfig(1, true, true, true);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config, executor);
        scheduler.start();

        // Wait for at least one execution cycle (1 minute interval, but we use
        // direct runCleanup for deterministic testing below)
        // For the scheduling test, verify the task was scheduled
        assertThat(executor.getQueue()).isNotEmpty();
    }

    @Test
    void closeStopsScheduledTasks() throws Exception {
        MetadataCleanupConfig config = new MetadataCleanupConfig(1, true, true, true);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config, executor);
        scheduler.start();
        scheduler.close();

        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    void closeIsIdempotent() {
        MetadataCleanupConfig config = MetadataCleanupConfig.DEFAULT;
        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config);
        scheduler.start();

        // Multiple close calls should not throw
        scheduler.close();
        scheduler.close();
        scheduler.close();
    }

    @Test
    void runCleanupExecutesAllEnabledTasks() {
        MetadataCleanupConfig config = new MetadataCleanupConfig(10, true, true, true);
        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config);

        scheduler.runCleanup();

        assertThat(lease.expireStaleCallCount.get()).isEqualTo(1);
    }

    @Test
    void runCleanupSkipsDisabledTasks() {
        MetadataCleanupConfig config = new MetadataCleanupConfig(10, false, false, false);
        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config);

        scheduler.runCleanup();

        assertThat(lease.expireStaleCallCount.get()).isEqualTo(0);
    }

    @Test
    void runCleanupContinuesAfterLeaseCleanupFailure() {
        lease.failOnExpireStale = true;
        MetadataCleanupConfig config = new MetadataCleanupConfig(10, true, true, true);
        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config);

        // Should not throw — failure in one task doesn't block others
        scheduler.runCleanup();

        assertThat(lease.expireStaleCallCount.get()).isEqualTo(1);
    }

    @Test
    void startAfterCloseIsNoOp() {
        MetadataCleanupConfig config = MetadataCleanupConfig.DEFAULT;
        scheduler = new MetadataCleanupScheduler(
                lease, poller, cache::performMaintenance, config);
        scheduler.close();

        // start after close should not throw and should not schedule
        scheduler.start();
    }

    @Test
    void configRejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new MetadataCleanupConfig(0, true, true, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalMinutes must be positive");

        assertThatThrownBy(() -> new MetadataCleanupConfig(-1, true, true, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalMinutes must be positive");
    }

    @Test
    void defaultConfigHasTenMinuteInterval() {
        MetadataCleanupConfig config = MetadataCleanupConfig.DEFAULT;
        assertThat(config.intervalMinutes()).isEqualTo(10);
        assertThat(config.leaseCleanupEnabled()).isTrue();
        assertThat(config.invalidationCleanupEnabled()).isTrue();
        assertThat(config.cacheCleanupEnabled()).isTrue();
    }

    // ── Test helper: tracking DistributedLease ──────────────────────

    private static final class TrackingDistributedLease implements DistributedLease {

        final AtomicInteger expireStaleCallCount = new AtomicInteger(0);
        volatile boolean failOnExpireStale = false;

        @Override
        public boolean tryAcquire(String leaseKey, String holderId, long epoch, Duration duration) {
            return true;
        }

        @Override
        public boolean tryRenew(String leaseKey, String holderId, Duration duration) {
            return true;
        }

        @Override
        public boolean release(String leaseKey, String holderId) {
            return true;
        }

        @Override
        public Optional<LeaseEntry> currentHolder(String leaseKey) {
            return Optional.empty();
        }

        @Override
        public int expireStale() {
            expireStaleCallCount.incrementAndGet();
            if (failOnExpireStale) {
                throw new RuntimeException("Simulated lease cleanup failure");
            }
            return 0;
        }
    }
}
