package org.pangrova.coordination.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Internal;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinationApiIsolationTest {
    @Test
    void allContractsLinkWithOnlyApiCommonAndJdk() throws Exception {
        URL[] artifacts = {CoordinationProvider.class.getProtectionDomain().getCodeSource().getLocation(),
                Internal.class.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new URLClassLoader(artifacts, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of("CoordinationProvider", "CoordinatorRegistry", "CoordinatorRole",
                    "CoordinatorSnapshot", "DistributedLease", "LeaseEntry", "WorkerRegistry", "WorkerSnapshot",
                    "HeartbeatSnapshot", "CacheCatalogSummary", "MaintenanceLeaderAware")) {
                var contract = loader.loadClass("org.pangrova.coordination.api." + name);
                assertThat(contract.getClassLoader()).isSameAs(loader);
                contract.getMethods();
                contract.getConstructors();
                contract.getRecordComponents();
            }
            for (String absent : List.of("org.pangrova.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.apache.curator.framework.CuratorFramework", "org.apache.zookeeper.ZooKeeper")) {
                assertThatThrownBy(() -> loader.loadClass(absent)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
