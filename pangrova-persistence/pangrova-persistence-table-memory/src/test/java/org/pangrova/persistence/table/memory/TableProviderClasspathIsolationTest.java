package org.pangrova.persistence.table.memory;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.table.memory.control.InMemoryTableControlStore;
import org.pangrova.persistence.table.memory.maintenance.InMemoryTableControlMaintenanceStore;
import org.pangrova.persistence.table.memory.statistics.InMemoryTableControlStatisticsStore;
import org.pangrova.persistence.table.memory.governancebinding.InMemoryTableControlGovernanceBindingStore;

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
                "org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore",
                "org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.security.authorization.deferred.DeferredAuthorization")) {
            assertThatThrownBy(() -> loader.loadClass(name)).as(name).isInstanceOf(ClassNotFoundException.class);
        }
        assertThat(new InMemoryTableControlStore()).isNotNull();
        assertThat(new InMemoryTableControlMaintenanceStore()).isNotNull();
        assertThat(new InMemoryTableControlStatisticsStore()).isNotNull();
        assertThat(new InMemoryTableControlGovernanceBindingStore()).isNotNull();
    }
}
