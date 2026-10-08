package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real store failure must precede creation of the lineage consumer during assembly. */
class GovernanceStoreInitializationIntegrationTest extends ContainerTestBase {
    @Test
    void invalidPersistentSchemaDoesNotStartAnUnownedLineageConsumer() {
        var postgres = SharedPostgresContainer.instance();
        var source = new PGSimpleDataSource();
        source.setURL(postgres.getJdbcUrl());
        source.setUser(postgres.getUsername());
        source.setPassword(postgres.getPassword());
        source.setCurrentSchema("missing_governance_" + UUID.randomUUID().toString().replace("-", ""));
        var defaults = CoordinatorBootstrapConfig.withDefaults("governance-init", 0);
        var config = new CoordinatorBootstrapConfig(defaults.core(),
                new CoordinatorBootstrapConfig.FeatureFlags(false, false, false, false, true, false, false, false),
                defaults.query(), defaults.transport(), defaults.workerHealth(), null, null, null);

        try (var modules = new ModuleIntegrationBootstrap()) {
            assertThatThrownBy(() -> modules.startGovernance(config, source))
                    .isInstanceOf(PangrovaException.class)
                    .satisfies(failure -> {
                        assertThat(((PangrovaException) failure).getErrorCode())
                                .isEqualTo(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED);
                        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("3F000");
                    });
            assertThat(modules.getLineageEmitter()).isNull();
        }
    }
}
