package org.pangrova.persistence.coordination.postgresql;

import org.pangrova.persistence.coordination.postgresql.membership.JdbcWorkerRegistry;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcCoordinatorRegistry;
import org.pangrova.persistence.coordination.postgresql.lease.JdbcDistributedLease;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.persistence.coordination.api.membership.WorkerRegistry;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the real owner test classpath without filtering or substituting its class loader. */
class JdbcCoordinationClasspathIsolationTest {
    @Test
    void providersConstructWithoutCoordinatorOrMetadataRuntimeAndFixtures() {
        var loader = getClass().getClassLoader();
        for (String unavailable : new String[]{
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService",
                "org.pangrova.coordinator.testinfra.SharedPostgresContainer",
                "org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository",
                "org.pangrova.metadata.relation.ObjectDependencyService"}) {
            assertThatThrownBy(() -> loader.loadClass(unavailable))
                    .as(unavailable).isInstanceOf(ClassNotFoundException.class);
        }

        var source = new PGSimpleDataSource();
        assertThat(new JdbcDistributedLease(source)).isInstanceOf(DistributedLease.class);
        assertThat(new JdbcCoordinatorRegistry(source)).isInstanceOf(CoordinatorRegistry.class);
        assertThat(new JdbcWorkerRegistry(source)).isInstanceOf(WorkerRegistry.class);
    }
}
