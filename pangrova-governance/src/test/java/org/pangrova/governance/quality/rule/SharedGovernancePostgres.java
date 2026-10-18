package org.pangrova.governance.quality.rule;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One PostgreSQL instance for governance integration tests in this JVM.
 * Individual tests use distinct schemas; the container is not stopped per class.
 */
final class SharedGovernancePostgres {

    static final PostgreSQLContainer<?> POSTGRES = startPostgres();

    private SharedGovernancePostgres() {
    }

    private static PostgreSQLContainer<?> startPostgres() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("pangrova_metadata_test")
                .withUsername("pangrova")
                .withPassword("pangrova_test");
        container.start();
        Runtime.getRuntime().addShutdownHook(new Thread(container::stop, "governance-postgres-shutdown"));
        return container;
    }
}
