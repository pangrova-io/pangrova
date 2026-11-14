package org.pangrova.server.coordinator.bootstrap;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.governance.api.exception.QualityRuleStoreException;
import org.pangrova.testing.testkit.ContainerTestBase;
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
            var registry = new org.pangrova.metadata.catalog.registry.CatalogRegistry(memoryTableStores(),
                    new org.pangrova.persistence.catalog.postgresql.catalog
                            .PostgresCatalogDeploymentRepository(source));
            assertThatThrownBy(() -> modules.startGovernance(config, source, GovernanceBootstrapTestAuthority.bind(modules, registry)))
                    .isInstanceOf(QualityRuleStoreException.class)
                    .hasMessageContaining("Cannot open canonical quality-rule schema")
                    .satisfies(failure -> {
                        assertThat(((PangrovaException) failure).getErrorCode())
                                .isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR);
                        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("42P01");
                    });
            assertThat(modules.getLineageEmitter()).isNull();
        }
    }
}
