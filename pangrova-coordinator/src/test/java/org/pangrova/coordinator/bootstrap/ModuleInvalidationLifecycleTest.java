package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.cache.JdbcInvalidationPoller;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.testing.ContainerTestBase;
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
            assertPollerTerminated(modules::setInvalidationPoller, modules);
        }
    }

    @Test
    void wiringShutdownJoinsOwnedInvalidationPoller() throws Exception {
        try (var modules = new ModuleIntegrationWiring()) {
            assertPollerTerminated(modules::setInvalidationPoller, modules);
        }
    }

    private static void assertPollerTerminated(
            Consumer<JdbcInvalidationPoller> register, AutoCloseable owner) throws Exception {
        var source = new TrackingPostgresSource(
                (PGSimpleDataSource) SharedPostgresContainer.migratedDataSource("poller_lifecycle"));
        try (var poller = new JdbcInvalidationPoller(source, new TieredMetadataCache())) {
            register.accept(poller);
            poller.initSchema();
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
