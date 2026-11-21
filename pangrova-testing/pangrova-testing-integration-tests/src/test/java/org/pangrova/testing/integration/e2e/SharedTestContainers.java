package org.pangrova.testing.integration.e2e;

import org.awaitility.core.ConditionTimeoutException;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

/**
 * JVM-level singleton containers (TestContainers Singleton Container Pattern).
 *
 * <p>The static initializer runs exactly once per classloader. With
 * {@code reuseForks=true}, all test classes in one surefire fork share
 * these instances. Container cleanup is handled by the consolidated
 * shutdown hook in {@link SqlE2ETestBase}.
 */
public final class SharedTestContainers {

    public static final boolean AVAILABLE;
    public static final Network NETWORK;
    public static final PostgreSQLContainer<?> POSTGRES;

    static {
        Network net = null;
        PostgreSQLContainer<?> pg = null;
        try {
            net = Network.newNetwork();
            pg = createPostgres(net);
            pg.start();
            probeJdbcReady(pg, Duration.ofSeconds(30));
        } catch (Throwable failure) {
            cleanupFailedStartup(pg, net, failure);
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Shared PostgreSQL startup or durability verification failed", failure);
        }
        AVAILABLE = true;
        NETWORK = net;
        POSTGRES = pg;
    }

    private static PostgreSQLContainer<?> createPostgres(Network network) {
        // The first ready log occurs during initdb, before the final listening server.
        return new PostgreSQLContainer<>("postgres:16-alpine")
                .withNetwork(network)
                .withDatabaseName("pangrova_meta")
                .withUsername("pangrova")
                .withPassword("pangrova_test")
                .withCommand("postgres",
                        "-c", "max_connections=200",
                        "-c", "shared_buffers=256MB",
                        "-c", "work_mem=8MB",
                        "-c", "maintenance_work_mem=64MB",
                        "-c", "effective_cache_size=512MB",
                        "-c", "checkpoint_timeout=30min",
                        "-c", "max_wal_size=2GB",
                        "-c", "log_min_duration_statement=10000",
                        "-c", "idle_in_transaction_session_timeout=60000",
                        "-c", "statement_timeout=120000")
                .withCreateContainerCmdModifier(cmd ->
                        cmd.getHostConfig().withShmSize(512L * 1024 * 1024))
                .withStartupAttempts(3)
                .withStartupTimeout(Duration.ofSeconds(120))
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(
                                ".*database system is ready to accept connections.*\\s", 2))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(120)));
    }

    private static void cleanupFailedStartup(
            PostgreSQLContainer<?> postgres, Network network, Throwable failure) {
        if (postgres != null) {
            try {
                postgres.stop();
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        if (network != null) {
            try {
                network.close();
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    static void probeJdbcReady(PostgreSQLContainer<?> pg, Duration timeout)
            throws Exception {
        AtomicReference<SQLException> lastFailure = new AtomicReference<>();
        try {
            await("shared-postgres-jdbc-ready")
                    .pollDelay(Duration.ZERO)
                    .pollInterval(Duration.ofMillis(200))
                    .atMost(timeout)
                    .until(new JdbcReadinessProbe(pg, lastFailure));
        } catch (ConditionTimeoutException timeoutFailure) {
            IllegalStateException failure = new IllegalStateException(
                    "PostgreSQL container did not become JDBC-ready within " + timeout,
                    timeoutFailure);
            if (lastFailure.get() != null) {
                failure.addSuppressed(lastFailure.get());
            }
            throw failure;
        }
        try (Connection connection = DriverManager.getConnection(
                pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
            verifyDurability(connection);
        }
    }

    /** Runs independently of the enclosing class's initialization monitor. */
    private static final class JdbcReadinessProbe implements Callable<Boolean> {
        private final PostgreSQLContainer<?> postgres;
        private final AtomicReference<SQLException> lastFailure;

        private JdbcReadinessProbe(PostgreSQLContainer<?> postgres,
                                  AtomicReference<SQLException> lastFailure) {
            this.postgres = postgres;
            this.lastFailure = lastFailure;
        }

        @Override
        public Boolean call() {
            try (Connection connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT 1")) {
                return rows.next() && rows.getInt(1) == 1;
            } catch (SQLException failure) {
                lastFailure.set(failure);
                return false;
            }
        }
    }

    static void verifyDurability(Connection connection) throws SQLException {
        Set<String> missing = new HashSet<>(Set.of(
                "fsync", "synchronous_commit", "full_page_writes"));
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT name, setting FROM pg_settings WHERE name IN "
                             + "('fsync', 'synchronous_commit', 'full_page_writes')")) {
            while (rows.next()) {
                String name = rows.getString("name");
                String setting = rows.getString("setting");
                if (!"on".equals(setting)) {
                    throw new IllegalStateException(
                            "PostgreSQL durability setting must be on: " + name + "=" + setting);
                }
                missing.remove(name);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing PostgreSQL durability settings: " + missing);
        }
    }

    private SharedTestContainers() {}
}
