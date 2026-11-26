package org.pangrova.connector.iceberg;

import org.testcontainers.containers.PostgreSQLContainer;

/** JVM-scoped PostgreSQL instance shared by Iceberg JDBC integration tests. */
final class IcebergJdbcTestDatabase {

    private IcebergJdbcTestDatabase() {}

    static PostgreSQLContainer<?> instance() {
        return Holder.POSTGRES;
    }

    private static final class Holder {
        private static final PostgreSQLContainer<?> POSTGRES = start();

        private static PostgreSQLContainer<?> start() {
            var postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_metadata_test")
                    .withUsername("pangrova").withPassword("pangrova_test")
                    .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements");
            postgres.start();
            return postgres;
        }
    }
}
