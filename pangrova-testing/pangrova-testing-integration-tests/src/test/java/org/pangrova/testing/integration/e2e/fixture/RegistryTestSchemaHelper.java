package org.pangrova.testing.integration.e2e.fixture;

import org.pangrova.testing.integration.e2e.SharedTestContainers;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Shared PostgreSQL access and cleanup helpers for HA registry E2E tests.
 *
 * <p>Schema creation is owned by the production Flyway migration chain.</p>
 */
public final class RegistryTestSchemaHelper {

    private RegistryTestSchemaHelper() {}

    /**
     * Returns the required shared PostgreSQL container or fails the E2E test.
     *
     * @return the running shared PostgreSQL container
     * @throws IllegalStateException if Docker or PostgreSQL is unavailable
     */
    public static PostgreSQLContainer<?> requirePostgres() {
        if (!SharedTestContainers.AVAILABLE
                || SharedTestContainers.POSTGRES == null
                || !SharedTestContainers.POSTGRES.isRunning()) {
            throw new IllegalStateException(
                    "Docker and the shared PostgreSQL container are required for registry E2E tests");
        }
        return SharedTestContainers.POSTGRES;
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
