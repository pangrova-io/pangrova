package org.pangrova.coordination.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.coordination.api.CoordinatorRegistry;
import org.pangrova.coordination.api.DistributedLease;
import org.pangrova.coordination.api.WorkerRegistry;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the real owner test classpath without filtering or substituting its class loader. */
class JdbcCoordinationClasspathIsolationTest {
    @Test
    void providersConstructWithoutCoordinatorOrMetadataRuntimeAndFixtures() {
        var loader = getClass().getClassLoader();
        for (String unavailable : new String[]{
                "org.pangrova.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService",
                "org.pangrova.coordinator.testinfra.SharedPostgresContainer",
                "org.pangrova.metadata.exception.MetastoreException",
                "org.pangrova.metadata.store.jdbc.catalog.EmbeddedDataSource",
                "org.pangrova.metadata.testinfra.SharedPostgresContainer"}) {
            assertThatThrownBy(() -> loader.loadClass(unavailable))
                    .as(unavailable).isInstanceOf(ClassNotFoundException.class);
        }

        var source = new PGSimpleDataSource();
        assertThat(new JdbcDistributedLease(source)).isInstanceOf(DistributedLease.class);
        assertThat(new JdbcCoordinatorRegistry(source)).isInstanceOf(CoordinatorRegistry.class);
        assertThat(new JdbcWorkerRegistry(source)).isInstanceOf(WorkerRegistry.class);
    }
}
