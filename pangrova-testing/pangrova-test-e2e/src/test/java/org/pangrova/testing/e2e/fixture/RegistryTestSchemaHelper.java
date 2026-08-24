package org.pangrova.testing.e2e.fixture;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Shared DDL helpers for HA registry E2E tests.
 *
 * <p>Eliminates duplicated {@code CREATE TABLE IF NOT EXISTS} and truncation
 * methods across {@code ClusterRegistryE2ETest}, {@code CoordinatorRegistryE2ETest},
 * and {@code WorkerRegistryE2ETest}.</p>
 */
public final class RegistryTestSchemaHelper {

    private RegistryTestSchemaHelper() {}

    public static void createWorkerRegistryTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS worker_registry (
                        worker_id VARCHAR(128) NOT NULL PRIMARY KEY,
                        host VARCHAR(256) NOT NULL,
                        http_port INT NOT NULL,
                        transport_port INT NOT NULL,
                        protocol_version INT NOT NULL DEFAULT 1,
                        capabilities TEXT,
                        rack_id VARCHAR(128) DEFAULT NULL,
                        data_locality TEXT,
                        memory_pressure VARCHAR(32) NOT NULL DEFAULT 'NONE',
                        cpu_utilization DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                        running_task_count INT NOT NULL DEFAULT 0,
                        available_memory_bytes BIGINT NOT NULL DEFAULT 0,
                        last_heartbeat_ms BIGINT NOT NULL,
                        registered_at_ms BIGINT NOT NULL
                    )""");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_wr_heartbeat ON worker_registry (last_heartbeat_ms)");
        }
    }

    public static void createCoordinatorRegistryTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS coordinator_registry (
                        coordinator_id VARCHAR(128) NOT NULL PRIMARY KEY,
                        host VARCHAR(256) NOT NULL,
                        http_port INT NOT NULL,
                        role VARCHAR(32) NOT NULL DEFAULT 'STANDBY',
                        epoch BIGINT NOT NULL DEFAULT 0,
                        last_heartbeat_ms BIGINT NOT NULL,
                        registered_at_ms BIGINT NOT NULL
                    )""");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cr_heartbeat ON coordinator_registry (last_heartbeat_ms)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cr_role ON coordinator_registry (role)");
        }
    }

    public static void createDistributedLeaseTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS distributed_lease (
                        lease_key   VARCHAR(255) PRIMARY KEY,
                        holder_id   VARCHAR(255) NOT NULL,
                        epoch       BIGINT       NOT NULL DEFAULT 0,
                        acquired_at TIMESTAMP(3) NOT NULL,
                        expires_at  TIMESTAMP(3) NOT NULL,
                        version     BIGINT       NOT NULL DEFAULT 0
                    )""");
        }
    }

    /**
     * Truncates the specified tables by executing {@code DELETE FROM <table>} for each.
     *
     * @param ds     the data source
     * @param tables table names to truncate
     */
    public static void truncateAll(DataSource ds, String... tables) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String table : tables) {
                stmt.execute("DELETE FROM " + table);
            }
        }
    }
}
