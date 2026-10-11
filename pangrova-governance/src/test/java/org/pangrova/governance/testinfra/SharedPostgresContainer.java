package org.pangrova.governance.testinfra;

import org.testcontainers.containers.PostgreSQLContainer;

/** JVM-wide actual PostgreSQL instance for independent governance SQL oracle tests. */
public final class SharedPostgresContainer {
    private static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private SharedPostgresContainer() {}

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    private static PostgreSQLContainer<?> startPostgres() {
        var container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("catalog")
                .withUsername("pangrova")
                .withPassword("pangrova_test");
        container.start();
        return container;
    }
}
