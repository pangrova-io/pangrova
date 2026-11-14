package org.pangrova.coordinator.governance;

import org.testcontainers.containers.PostgreSQLContainer;

/** JVM-wide actual PostgreSQL instance for structured quality SQL oracle tests. */
final class QualityPostgres {
    static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private QualityPostgres() {}

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
