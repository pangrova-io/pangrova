package org.pangrova.testing.e2e;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

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
        boolean ok = false;
        Network net = null;
        PostgreSQLContainer<?> pg = null;
        try {
            net = Network.newNetwork();
            // PG container occasionally died mid-suite under load.
            // Tuning: lowered max_connections to 200 (E2E rarely needs >50),
            // bumped shared_buffers, lowered work_mem to prevent OOM-kills,
            // increased shm to 512MB for hash joins, and tagged startup with
            // up to 3 attempts. Wait strategy = log("ready" *2nd* occurrence)
            // + listening port, because the first "ready" log line appears
            // during initdb, not at the real listen point.
            pg = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withNetwork(net)
                    .withDatabaseName("pangrova_meta")
                    .withUsername("pangrova")
                    .withPassword("pangrova_test")
                    .withCommand("postgres",
                            "-c", "max_connections=200",
                            "-c", "shared_buffers=256MB",
                            "-c", "work_mem=8MB",
                            "-c", "maintenance_work_mem=64MB",
                            "-c", "effective_cache_size=512MB",
                            "-c", "fsync=off",
                            "-c", "synchronous_commit=off",
                            "-c", "full_page_writes=off",
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
            pg.start();
            // Belt-and-suspenders: PG can briefly close TCP during SSL
            // handshake right after "ready". Probe with a real JDBC SELECT 1.
            probeJdbcReady(pg, Duration.ofSeconds(30));
            ok = true;
        } catch (Throwable e) {
            System.err.println("[SharedTestContainers] FAILED to start PostgreSQL: " + e);
            e.printStackTrace(System.err);
            if (pg != null) {
                try { pg.stop(); } catch (Exception ignored) { }
                pg = null;
            }
            if (net != null) {
                try { net.close(); } catch (Exception ignored) { }
                net = null;
            }
        }
        AVAILABLE = ok;
        NETWORK = net;
        POSTGRES = pg;
    }

    static void probeJdbcReady(PostgreSQLContainer<?> pg, Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try (Connection conn = DriverManager.getConnection(
                    pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT 1")) {
                if (rs.next() && rs.getInt(1) == 1) {
                    return;
                }
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(200L);
        }
        throw new IllegalStateException(
                "PostgreSQL container did not become JDBC-ready within " + timeout, last);
    }

    private SharedTestContainers() {}
}
