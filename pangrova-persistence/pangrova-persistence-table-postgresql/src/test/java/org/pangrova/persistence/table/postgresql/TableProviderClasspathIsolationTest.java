package org.pangrova.persistence.table.postgresql;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.persistence.table.postgresql.maintenance.JdbcTableControlMaintenanceStore;
import org.pangrova.persistence.table.postgresql.statistics.JdbcTableControlStatisticsStore;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the actual owner classpath, without filtering away accidental dependencies. */
class TableProviderClasspathIsolationTest {
    @Test
    void storesLinkWithTheFlatTableArtifactWithoutServerOrAlternateBackends() throws Exception {
        var loader = getClass().getClassLoader();
        var contract = loader.loadClass("org.pangrova.table.api.ownership.TableControlRecord");
        var service = loader.loadClass("org.pangrova.table.ownership.TableControlService");
        assertThat(service.getProtectionDomain().getCodeSource().getLocation())
                .isEqualTo(contract.getProtectionDomain().getCodeSource().getLocation());
        for (String name : List.of(
                "org.pangrova.persistence.table.memory.control.InMemoryTableControlStore",
                "org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.security.authorization.deferred.DeferredAuthorization")) {
            assertThatThrownBy(() -> loader.loadClass(name)).as(name).isInstanceOf(ClassNotFoundException.class);
        }
        var source = new org.postgresql.ds.PGSimpleDataSource();
        assertThat(new JdbcTableControlStore(source)).isNotNull();
        assertThat(new JdbcTableControlMaintenanceStore(source)).isNotNull();
        assertThat(new JdbcTableControlStatisticsStore(source)).isNotNull();
        assertThat(new PostgresTablePolicyRepository(source)).isNotNull();
    }
}
