package org.pangrova.server.coordinator.bootstrap;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.jdbc.datasource.DriverManagerDataSource;
import org.pangrova.persistence.table.api.provider.TableStoreFactory;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.persistence.table.postgresql.maintenance.JdbcTableControlMaintenanceStore;
import org.pangrova.persistence.table.postgresql.statistics.JdbcTableControlStatisticsStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableStoreDiscoveryTest {
    @Test
    void coordinatorResolvesTheEntirePostgresAuthority() {
        try (var source = new DriverManagerDataSource("jdbc:unused", "", "")) {
            var stores = CoordinatorBootstrap.loadTableStores(getClass().getClassLoader(), source);
            assertThat(stores.control()).isInstanceOf(JdbcTableControlStore.class);
            assertThat(stores.statistics()).isInstanceOf(JdbcTableControlStatisticsStore.class);
            assertThat(stores.governance()).isInstanceOf(PostgresTablePolicyRepository.class);
            assertThat(stores.maintenance()).isInstanceOf(JdbcTableControlMaintenanceStore.class);
        }
    }

    @Test
    void incompatibleInstalledFactoryRejectsStartupBeforeRepositoryCreation(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var path = directory.resolve("META-INF/services/" + TableStoreFactory.class.getName());
        java.nio.file.Files.createDirectories(path.getParent());
        java.nio.file.Files.writeString(path, FutureStoreFactory.class.getName());
        try (var loader = new java.net.URLClassLoader(new URL[]{directory.toUri().toURL()}, getClass().getClassLoader());
             var source = new DriverManagerDataSource("jdbc:unused", "", "")) {
            assertThatThrownBy(() -> CoordinatorBootstrap.loadTableStores(loader, source))
                    .hasMessageContaining("Incompatible table storage provider: future");
        }
    }

    public static final class FutureStoreFactory implements TableStoreFactory {
        public String identifier() { return "future"; }
        public int apiVersion() { return API_VERSION + 1; }
        public org.pangrova.persistence.table.api.provider.TableStores create(java.util.Optional<javax.sql.DataSource> source) {
            throw new AssertionError("Incompatible provider created stores");
        }
    }

    @Test
    void missingDescriptorFailsStartup() {
        String descriptor = "META-INF/services/" + TableStoreFactory.class.getName();
        ClassLoader noTableProviders = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Enumeration<URL> getResources(String name) throws IOException {
                return descriptor.equals(name) ? Collections.emptyEnumeration() : super.getResources(name);
            }
        };
        try (var source = new DriverManagerDataSource("jdbc:unused", "", "")) {
            assertThatThrownBy(() -> CoordinatorBootstrap.loadTableStores(noTableProviders, source))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Missing PostgreSQL table storage provider");
        }
    }
}
