package org.pangrova.persistence.table.postgresql;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.persistence.table.postgresql.maintenance.JdbcTableControlMaintenanceStore;
import org.pangrova.persistence.table.postgresql.statistics.JdbcTableControlStatisticsStore;
import org.pangrova.persistence.table.postgresql.governancebinding.JdbcTableControlGovernanceBindingStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the actual owner classpath, without filtering away accidental dependencies. */
class TableProviderClasspathIsolationTest {
    @Test
    void allProvidersConstructWithoutDomainOrProcessRuntime() {
        var loader = getClass().getClassLoader();
        for (String name : List.of("org.pangrova.table.core.ownership.TableControlService",
                "org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.security.core.authorization.deferred.DeferredAuthorization")) {
            assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
        }
        var source = new org.postgresql.ds.PGSimpleDataSource();
        assertThat(new JdbcTableControlStore(source)).isNotNull();
        assertThat(new JdbcTableControlMaintenanceStore(source)).isNotNull();
        assertThat(new JdbcTableControlStatisticsStore(source)).isNotNull();
        assertThat(new JdbcTableControlGovernanceBindingStore(source)).isNotNull();
    }
}
