package org.pangrova.metadata.service;

import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for MetadataCleanupScheduler lifecycle and cleanup execution.
 */
class MetadataCleanupSchedulerTest {

    private DataSource dataSource;
    private PostgresInvalidationPoller poller;
    private TieredMetadataCache cache;
    private final AtomicInteger cacheMaintenanceCalls = new AtomicInteger();
    private MetadataCleanupScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("cleanup_scheduler");
        cache = new TieredMetadataCache();
        poller = new PostgresInvalidationPoller(dataSource, cache);
        poller.initSchema();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (scheduler != null) {
            scheduler.close();
        }
        poller.close();
    }

    @Test
    void startSchedulesPeriodicCleanup() throws Exception {
        MetadataCleanupConfig config = new MetadataCleanupConfig(1, true, true);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        scheduler = new MetadataCleanupScheduler(
                poller, cache::performMaintenance, config, executor);
        scheduler.start();

        // Wait for at least one execution cycle (1 minute interval, but we use
        // direct runCleanup for deterministic testing below)
        // For the scheduling test, verify the task was scheduled
        assertThat(executor.getQueue()).isNotEmpty();
    }

    @Test
    void closeStopsScheduledTasks() throws Exception {
        MetadataCleanupConfig config = new MetadataCleanupConfig(1, true, true);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        scheduler = new MetadataCleanupScheduler(
                poller, cache::performMaintenance, config, executor);
        scheduler.start();
        scheduler.close();

        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    void closeIsIdempotent() {
        MetadataCleanupConfig config = MetadataCleanupConfig.DEFAULT;
        scheduler = new MetadataCleanupScheduler(
                poller, cache::performMaintenance, config);
        scheduler.start();

        // Multiple close calls should not throw
        scheduler.close();
        scheduler.close();
        scheduler.close();
    }

    @Test
    void runCleanupExecutesAllEnabledTasks() {
        MetadataCleanupConfig config = new MetadataCleanupConfig(10, true, true);
        scheduler = new MetadataCleanupScheduler(
                poller, cacheMaintenanceCalls::incrementAndGet, config);

        scheduler.runCleanup();

        assertThat(cacheMaintenanceCalls.get()).isEqualTo(1);
    }

    @Test
    void runCleanupSkipsDisabledTasks() {
        MetadataCleanupConfig config = new MetadataCleanupConfig(10, false, false);
        scheduler = new MetadataCleanupScheduler(
                poller, cacheMaintenanceCalls::incrementAndGet, config);

        scheduler.runCleanup();

        assertThat(cacheMaintenanceCalls.get()).isEqualTo(0);
    }

    @Test
    void startAfterCloseIsNoOp() {
        MetadataCleanupConfig config = MetadataCleanupConfig.DEFAULT;
        scheduler = new MetadataCleanupScheduler(
                poller, cache::performMaintenance, config);
        scheduler.close();

        // start after close should not throw and should not schedule
        scheduler.start();
    }

    @Test
    void configRejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new MetadataCleanupConfig(0, true, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalMinutes must be positive");

        assertThatThrownBy(() -> new MetadataCleanupConfig(-1, true, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalMinutes must be positive");
    }

    @Test
    void defaultConfigHasTenMinuteInterval() {
        MetadataCleanupConfig config = MetadataCleanupConfig.DEFAULT;
        assertThat(config.intervalMinutes()).isEqualTo(10);
        assertThat(config.invalidationCleanupEnabled()).isTrue();
        assertThat(config.cacheCleanupEnabled()).isTrue();
    }
}
