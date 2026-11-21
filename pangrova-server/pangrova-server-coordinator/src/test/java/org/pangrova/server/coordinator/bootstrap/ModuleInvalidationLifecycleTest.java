package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Uses an actual JDBC poll to observe the owned scheduler thread, not thread-name enumeration. */
class ModuleInvalidationLifecycleTest extends ContainerTestBase {
    @Test
    void bootstrapShutdownJoinsOwnedInvalidationPoller() throws Exception {
        try (var modules = new ModuleIntegrationBootstrap()) {
            assertPollerTerminated(modules.catalogMaintenance()::setInvalidationPoller, modules);
        }
    }

    @Test
    void successiveLeadershipTermsOwnOneCleanupTaskAndReleaseTheExecutor() throws Exception {
        var executor = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);
        try (var modules = new ModuleIntegrationBootstrap(executor)) {
            var data = SharedPostgresContainer.migratedDataSource("cleanup_leadership");
            modules.catalogMaintenance().setInvalidationPoller(new PostgresInvalidationPoller(data, new TieredMetadataCache()));
            modules.onLeaderElected();
            modules.onLeaderElected();
            assertThat(executor.getQueue()).hasSize(1);
            modules.onLeadershipLost();
            assertThat(executor.getQueue()).isEmpty();
            modules.onLeaderElected();
            assertThat(executor.getQueue()).hasSize(1);
        }
        assertThat(executor.isTerminated()).isTrue();
    }

    @Test
    void electionCanBeRetriedAfterTheCatalogSchedulerRejectsStartup() throws Exception {
        var rejectFirst = new java.util.concurrent.atomic.AtomicBoolean(true);
        var executor = new java.util.concurrent.ScheduledThreadPoolExecutor(1) {
            @Override
            public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(Runnable action,
                    long initialDelay, long delay, java.util.concurrent.TimeUnit unit) {
                if (rejectFirst.getAndSet(false)) throw new java.util.concurrent.RejectedExecutionException("injected");
                return super.scheduleWithFixedDelay(action, initialDelay, delay, unit);
            }
        };
        executor.setRemoveOnCancelPolicy(true);
        try (var modules = new ModuleIntegrationBootstrap(executor)) {
            var data = SharedPostgresContainer.migratedDataSource("failed_leadership");
            modules.catalogMaintenance().setInvalidationPoller(new PostgresInvalidationPoller(data, new TieredMetadataCache()));
            org.assertj.core.api.Assertions.assertThatThrownBy(modules::onLeaderElected)
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class).hasMessage("injected");
            assertThat(executor.getQueue()).isEmpty();
            modules.onLeaderElected();
            assertThat(executor.getQueue()).hasSize(1);
            modules.onLeadershipLost();
            assertThat(executor.getQueue()).isEmpty();
        }
    }

    private static void assertPollerTerminated(
            Consumer<PostgresInvalidationPoller> register, AutoCloseable owner) throws Exception {
        var source = new TrackingPostgresSource(
                (PGSimpleDataSource) SharedPostgresContainer.migratedDataSource("poller_lifecycle"));
        try (var poller = new PostgresInvalidationPoller(source, new TieredMetadataCache())) {
            register.accept(poller);
            poller.validateSchema();
            poller.start();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(source.pollingThread.get()).isNotNull());
            Thread worker = source.pollingThread.get();
            assertThat(worker.isAlive()).isTrue();

            owner.close();

            // Executor TERMINATED is signaled while the worker is unwinding its final stack frame.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(worker.isAlive()).as("Owner must stop its owned polling thread").isFalse());
            owner.close();
            assertThat(worker.isAlive()).isFalse();
        }
    }

    private static final class TrackingPostgresSource extends PGSimpleDataSource {
        private final AtomicReference<Thread> pollingThread = new AtomicReference<>();

        private TrackingPostgresSource(PGSimpleDataSource source) {
            setURL(source.getURL());
            setCurrentSchema(source.getCurrentSchema());
            setUser(source.getUser());
            setPassword(source.getPassword());
        }

        @Override
        public Connection getConnection() throws SQLException {
            if ("invalidation-poller".equals(Thread.currentThread().getName())) {
                pollingThread.set(Thread.currentThread());
            }
            return super.getConnection();
        }
    }
}
