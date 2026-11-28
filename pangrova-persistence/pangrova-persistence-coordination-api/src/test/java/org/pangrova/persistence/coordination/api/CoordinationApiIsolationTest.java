package org.pangrova.persistence.coordination.api;

import org.pangrova.persistence.coordination.api.membership.WorkerRegistry;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.persistence.coordination.api.provider.CoordinationProvider;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinationApiIsolationTest {
    @Test
    void allCoordinationContractsLinkWithoutBackendOrProcessImplementations() throws Exception {
        URL[] artifacts = {CoordinationProvider.class.getProtectionDomain().getCodeSource().getLocation(),
                Internal.class.getProtectionDomain().getCodeSource().getLocation(),
                CoordinatorSnapshot.class.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new URLClassLoader(artifacts, ClassLoader.getPlatformClassLoader())) {
            for (Class<?> declared : List.of(org.pangrova.persistence.coordination.api.exception.CoordinationStorageException.class, CoordinationProvider.class, CoordinatorRegistry.class,
                    org.pangrova.persistence.coordination.api.admission.ClusterConcurrencySlots.class,
                    org.pangrova.coordination.api.enums.CoordinatorRole.class,
                    CoordinatorSnapshot.class, DistributedLease.class,
                    org.pangrova.coordination.api.lease.LeaseEntry.class, WorkerRegistry.class,
                    org.pangrova.coordination.api.membership.WorkerSnapshot.class,
                    org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot.class,
                    org.pangrova.coordination.api.heartbeat.CacheCatalogSummary.class,
                    org.pangrova.coordination.api.leadership.MaintenanceLeadershipListener.class)) {
                var contract = loader.loadClass(declared.getName());
                assertThat(contract.getClassLoader()).isSameAs(loader);
                contract.getMethods();
                contract.getConstructors();
                contract.getRecordComponents();
            }
            assertThat(CoordinatorRegistry.class.getMethod("updateLastProcessedInvalidationId", String.class, long.class)
                    .isDefault()).as("invalidation progress cannot silently acknowledge an unsupported write").isFalse();
            for (String absent : List.of("org.pangrova.table.api.ownership.TableControlStore",
                    "org.pangrova.persistence.coordination.postgresql.membership.JdbcWorkerRegistry",
                    "org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.apache.curator.framework.CuratorFramework", "org.apache.zookeeper.ZooKeeper")) {
                assertThatThrownBy(() -> loader.loadClass(absent)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
