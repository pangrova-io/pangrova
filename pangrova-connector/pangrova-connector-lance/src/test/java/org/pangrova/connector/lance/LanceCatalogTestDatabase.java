package org.pangrova.connector.lance;

import org.testcontainers.containers.PostgreSQLContainer;

/** JVM-scoped real database shared by Lance catalog integration tests. */
final class LanceCatalogTestDatabase {
    private LanceCatalogTestDatabase() {}

    static PostgreSQLContainer<?> instance() { return Holder.POSTGRES; }

    private static final class Holder {
        private static final PostgreSQLContainer<?> POSTGRES = start();

        private static PostgreSQLContainer<?> start() {
            var postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_metadata_test").withUsername("pangrova").withPassword("pangrova_test");
            postgres.start();
            return postgres;
        }
    }
}
